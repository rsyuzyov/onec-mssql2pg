package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Режимы prepare и restore: временная настройка сервера PostgreSQL под массовую заливку и возврат.
//
// prepare запоминает прежние значения в файле состояния, ставит настройки через ALTER SYSTEM,
// выключает автоочистку на таблицах целевой базы и, если настройке нужен перезапуск, выполняет
// -restart-cmd (или завершается с кодом 3 — перезапусти и запусти prepare еще раз).
// restore возвращает все, что записано в файле состояния.

// errRestart — нужен перезапуск сервера, а команды перезапуска нет.
var errRestart = errors.New("нужен перезапуск PostgreSQL")

// loadSettings — настройки сервера на время заливки. По умолчанию выключаются и fsync с
// full_page_writes: пока перенос не закончен, база не боевая, и сбой посреди заливки означает
// заливку заново. -safe оставляет их включенными.
func loadSettings(safe bool, mwm string, maintWorkers int) [][2]string {
	s := [][2]string{
		{"wal_level", "minimal"},
		{"max_wal_senders", "0"},
		{"archive_mode", "off"},
		{"max_wal_size", "64GB"},
		{"checkpoint_timeout", "60min"},
		{"synchronous_commit", "off"},
		{"autovacuum", "off"},
		{"maintenance_work_mem", mwm},
		{"max_parallel_maintenance_workers", fmt.Sprint(maintWorkers)},
		{"wal_skip_threshold", "0"},
	}
	if !safe {
		s = append(s, [2]string{"fsync", "off"}, [2]string{"full_page_writes", "off"})
	}
	return s
}

// prodSafe — что restore включает всегда, каким бы ни было значение до prepare: после переноса
// база становится боевой.
var prodSafe = map[string]string{"fsync": "on", "full_page_writes": "on", "autovacuum": "on"}

type savedSetting struct {
	Name     string `json:"name"`
	Value    string `json:"value"`     // значение до prepare, как его покажет current_setting
	AutoConf bool   `json:"auto_conf"` // было задано через ALTER SYSTEM: при возврате SET, иначе RESET
}

type pgState struct {
	Saved    []savedSetting `json:"settings"`
	Database string         `json:"database"`
	Tables   []string       `json:"tables_autovacuum_off"`
	At       time.Time      `json:"prepared_at"`
}

func autoConfNames(ctx context.Context, pg *pgxpool.Pool) (map[string]bool, error) {
	rows, err := pg.Query(ctx, `select name from pg_file_settings where sourcefile like '%postgresql.auto.conf' and applied`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	res := map[string]bool{}
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		res[n] = true
	}
	return res, rows.Err()
}

func pendingRestart(ctx context.Context, pg *pgxpool.Pool) ([]string, error) {
	rows, err := pg.Query(ctx, `select name from pg_settings where pending_restart`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		res = append(res, n)
	}
	return res, rows.Err()
}

// restartIfNeeded перезапускает сервер командой cmd, если есть настройки, ждущие перезапуска.
func restartIfNeeded(ctx context.Context, pg *pgxpool.Pool, cmd string) error {
	pend, err := pendingRestart(ctx, pg)
	if err != nil || len(pend) == 0 {
		return err
	}
	if cmd == "" {
		log.Printf("ждут перезапуска: %s", strings.Join(pend, ", "))
		return errRestart
	}
	log.Printf("перезапуск PostgreSQL (%s) ради: %s", cmd, strings.Join(pend, ", "))
	pg.Reset()
	if out, err := exec.Command("sh", "-c", cmd).CombinedOutput(); err != nil {
		return fmt.Errorf("перезапуск: %v: %s", err, out)
	}
	for i := 0; i < 60; i++ {
		if err := pg.Ping(ctx); err == nil {
			return nil
		}
		time.Sleep(2 * time.Second)
	}
	return errors.New("после перезапуска PostgreSQL не отвечает")
}

func runPrepare(ctx context.Context, pg *pgxpool.Pool, stateFile, restartCmd string, safe bool, mwm string, maintWorkers int) error {
	var st pgState
	if b, err := os.ReadFile(stateFile); err == nil {
		// Повторный запуск (после перезапуска): прежние значения уже сохранены, не затираем их.
		if err := json.Unmarshal(b, &st); err != nil {
			return fmt.Errorf("%s: %w", stateFile, err)
		}
		log.Printf("состояние уже сохранено %s — продолжаю prepare", st.At.Format(time.DateTime))
	} else {
		auto, err := autoConfNames(ctx, pg)
		if err != nil {
			return err
		}
		for _, s := range loadSettings(safe, mwm, maintWorkers) {
			var v string
			if err := pg.QueryRow(ctx, "select current_setting($1)", s[0]).Scan(&v); err != nil {
				return fmt.Errorf("%s: %w", s[0], err)
			}
			st.Saved = append(st.Saved, savedSetting{Name: s[0], Value: v, AutoConf: auto[s[0]]})
		}
		if err := pg.QueryRow(ctx, "select current_database()").Scan(&st.Database); err != nil {
			return err
		}
		st.At = time.Now()
		b, _ := json.MarshalIndent(st, "", "  ")
		if err := os.WriteFile(stateFile, b, 0o600); err != nil {
			return err
		}
		log.Printf("прежние значения сохранены в %s", stateFile)
	}
	for _, s := range loadSettings(safe, mwm, maintWorkers) {
		if _, err := pg.Exec(ctx, fmt.Sprintf("alter system set %s = '%s'", s[0], s[1])); err != nil {
			return fmt.Errorf("alter system %s: %w", s[0], err)
		}
	}
	if _, err := pg.Exec(ctx, "select pg_reload_conf()"); err != nil {
		return err
	}
	time.Sleep(time.Second)
	if err := restartIfNeeded(ctx, pg, restartCmd); err != nil {
		return err
	}
	// Автоочистка по таблицам: глобальный autovacuum=off не мешает запуску от переполнения счетчика
	// транзакций, а табличный параметр отключает очистку этой таблицы и в анализе.
	rows, err := pg.Query(ctx, `select c.relname from pg_class c join pg_namespace n on n.oid = c.relnamespace
		where n.nspname = 'public' and c.relkind = 'r'
		and not coalesce('autovacuum_enabled=false' = any(c.reloptions), false)`)
	if err != nil {
		return err
	}
	var tables []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			rows.Close()
			return err
		}
		tables = append(tables, t)
	}
	rows.Close()
	for _, t := range tables {
		if _, err := pg.Exec(ctx, fmt.Sprintf("alter table %s set (autovacuum_enabled = false)", qpg(t))); err != nil {
			return fmt.Errorf("%s: %w", t, err)
		}
	}
	st.Tables = append(st.Tables, tables...)
	b, _ := json.MarshalIndent(st, "", "  ")
	if err := os.WriteFile(stateFile, b, 0o600); err != nil {
		return err
	}
	return reportSettings(ctx, pg, loadSettings(safe, mwm, maintWorkers), "prepare")
}

func runRestore(ctx context.Context, pg *pgxpool.Pool, stateFile, restartCmd string) error {
	b, err := os.ReadFile(stateFile)
	if err != nil {
		return fmt.Errorf("нет файла состояния: %w", err)
	}
	var st pgState
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	for _, t := range st.Tables {
		if _, err := pg.Exec(ctx, fmt.Sprintf("alter table %s reset (autovacuum_enabled)", qpg(t))); err != nil {
			log.Printf("%s: %v", t, err)
		}
	}
	var want [][2]string
	saved := map[string]bool{}
	for _, s := range st.Saved {
		saved[s.Name] = true
	}
	// Безопасные значения — и для настроек, которые prepare не трогал, но они были выключены вручную.
	for name := range prodSafe {
		if !saved[name] {
			st.Saved = append(st.Saved, savedSetting{Name: name, Value: prodSafe[name], AutoConf: true})
		}
	}
	for _, s := range st.Saved {
		if v, ok := prodSafe[s.Name]; ok && !sameSetting(s.Value, v) {
			log.Printf("restore: %s было %s до prepare — включаю %s (база станет боевой)", s.Name, s.Value, v)
			s.Value, s.AutoConf = v, true
		}
		q := fmt.Sprintf("alter system reset %s", s.Name)
		if s.AutoConf {
			q = fmt.Sprintf("alter system set %s = '%s'", s.Name, s.Value)
		}
		if _, err := pg.Exec(ctx, q); err != nil {
			return fmt.Errorf("%s: %w", s.Name, err)
		}
		want = append(want, [2]string{s.Name, s.Value})
	}
	if _, err := pg.Exec(ctx, "select pg_reload_conf()"); err != nil {
		return err
	}
	time.Sleep(time.Second)
	if err := restartIfNeeded(ctx, pg, restartCmd); err != nil {
		return err
	}
	// Файлы, записанные при fsync=off, PostgreSQL сам не досинхронизирует даже после включения
	// fsync: сбросить их на диск — sync на уровне ОС (документация PostgreSQL, параметр fsync).
	if _, err := pg.Exec(ctx, "checkpoint"); err != nil {
		return err
	}
	if out, err := exec.Command("sync").CombinedOutput(); err != nil {
		return fmt.Errorf("sync: %v: %s", err, out)
	}
	log.Print("restore: checkpoint и sync выполнены")
	if err := reportSettings(ctx, pg, want, "restore"); err != nil {
		return err
	}
	return os.Rename(stateFile, stateFile+".restored-"+time.Now().Format("20060102-150405"))
}

// reportSettings сверяет фактические значения с ожидаемыми.
func reportSettings(ctx context.Context, pg *pgxpool.Pool, want [][2]string, what string) error {
	bad := 0
	for _, s := range want {
		var v string
		if err := pg.QueryRow(ctx, "select current_setting($1)", s[0]).Scan(&v); err != nil {
			return err
		}
		if !sameSetting(v, s[1]) {
			bad++
			log.Printf("%s: %s = %s, ожидалось %s", what, s[0], v, s[1])
		}
	}
	if bad > 0 {
		return fmt.Errorf("%s: не применилось настроек: %d", what, bad)
	}
	log.Printf("%s: все %d настроек на месте", what, len(want))
	return nil
}

// sameSetting — current_setting показывает 64GB как "64GB", а 60min как "1h".
func sameSetting(a, b string) bool {
	if strings.EqualFold(a, b) {
		return true
	}
	norm := map[string]string{"60min": "1h", "0": "0", "64GB": "64GB"}
	return norm[b] == a
}
