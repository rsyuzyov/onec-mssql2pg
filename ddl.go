package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ddlStmt — одна команда из файла и таблица, к которой она относится.
type ddlStmt struct {
	sql, table string
	pk         bool
}

// parseDDL режет вывод pg_dump --section=post-data на команды. Служебные SET/SELECT
// и комментарии отбрасываются.
func parseDDL(text string) []ddlStmt {
	var out []ddlStmt
	for _, raw := range strings.Split(text, ";\n") {
		var lines []string
		for _, l := range strings.Split(raw, "\n") {
			t := strings.TrimSpace(l)
			if t == "" || strings.HasPrefix(t, "--") || strings.HasPrefix(t, `\`) {
				continue
			}
			lines = append(lines, l)
		}
		s := strings.TrimSpace(strings.Join(lines, "\n"))
		u := strings.ToUpper(s)
		if s == "" || strings.HasPrefix(u, "SET ") || strings.HasPrefix(u, "SELECT ") {
			continue
		}
		st := ddlStmt{sql: s}
		f := strings.Fields(s)
		for i, w := range f {
			if (strings.EqualFold(w, "ON") || strings.EqualFold(w, "ONLY")) && i+1 < len(f) {
				st.table = strings.TrimPrefix(f[i+1], "public.")
				break
			}
		}
		st.pk = strings.Contains(u, "PRIMARY KEY")
		out = append(out, st)
	}
	return out
}

// runDDL выполняет команды параллельно. Команды одной таблицы идут одна за другой в одном
// потоке: ALTER TABLE ... PRIMARY KEY берет исключительную блокировку и все равно ждал бы
// соседние построения индексов. Большие таблицы запускаются первыми.
func runDDL(ctx context.Context, pg *pgxpool.Pool, file string, workers int) error {
	b, err := os.ReadFile(file)
	if err != nil {
		return err
	}
	stmts := parseDDL(string(b))
	byTable := map[string][]ddlStmt{}
	for _, s := range stmts {
		byTable[s.table] = append(byTable[s.table], s)
	}
	size := map[string]int64{}
	rows, err := pg.Query(ctx, `select c.relname, pg_relation_size(c.oid) from pg_class c
		join pg_namespace n on n.oid = c.relnamespace where n.nspname = 'public' and c.relkind = 'r'`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var n string
		var sz int64
		if err := rows.Scan(&n, &sz); err != nil {
			rows.Close()
			return err
		}
		size[n] = sz
	}
	rows.Close()
	var tables []string
	for t := range byTable {
		tables = append(tables, t)
	}
	sort.Slice(tables, func(i, j int) bool { return size[tables[i]] > size[tables[j]] })
	log.Printf("ddl: команд %d, таблиц %d, потоков %d", len(stmts), len(tables), workers)

	start := time.Now()
	var done atomic.Int64
	var errMu sync.Mutex
	var errs []string
	ch := make(chan string)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := pg.Acquire(ctx)
			if err != nil {
				errMu.Lock()
				errs = append(errs, err.Error())
				errMu.Unlock()
				return
			}
			defer conn.Release()
			for t := range ch {
				list := byTable[t]
				// Первичный ключ — первым: он же строит уникальный индекс.
				sort.SliceStable(list, func(i, j int) bool { return list[i].pk && !list[j].pk })
				t0 := time.Now()
				for _, s := range list {
					if _, err := conn.Exec(ctx, s.sql); err != nil {
						errMu.Lock()
						errs = append(errs, fmt.Sprintf("%s: %v", t, err))
						errMu.Unlock()
						log.Printf("ОШИБКА %s: %v", t, err)
					}
					done.Add(1)
				}
				if d := time.Since(t0); d > 30*time.Second {
					log.Printf("индексы %s: %d команд за %s", t, len(list), d.Round(time.Second))
				}
			}
		}()
	}
	for _, t := range tables {
		ch <- t
	}
	close(ch)
	wg.Wait()
	log.Printf("ddl: выполнено %d команд за %s, ошибок %d", done.Load(), time.Since(start).Round(time.Second), len(errs))
	if len(errs) > 0 {
		return fmt.Errorf("ошибок: %d, первая: %s", len(errs), errs[0])
	}
	return nil
}
