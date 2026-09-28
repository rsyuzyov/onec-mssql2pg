// onec-mssql2pg переносит данные информационной базы 1С из MS SQL Server в PostgreSQL.
//
// Структура базы в PostgreSQL (таблицы 1С, служебные таблицы, функции) должна быть создана
// заранее, например pg_dump --schema-only с базы, построенной платформой. Утилита переносит
// только данные: читает таблицы MS SQL параллельно, большие — диапазонами ключа, и пишет
// в PostgreSQL через COPY. Индексы строятся отдельно, после заливки.
package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"runtime/debug"
	"runtime/pprof"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/microsoft/go-mssqldb"
)

func main() {
	mode := flag.String("mode", "copy", "copy | plan | splits | verify-counts | verify-pg | ddl | prepare | restore | delta | cdc-enable | cdc-disable")
	workers := flag.Int("workers", 24, "число параллельных потоков")
	splitRows := flag.Int64("split-rows", 20_000_000, "таблицы больше этого числа строк делятся на части по ключу")
	splitMB := flag.Int64("split-mb", 2048, "таблицы больше этого объема (МБ, с LOB) делятся на части по ключу")
	ddlFile := flag.String("ddl", "", "режим ddl: файл с командами (CREATE INDEX, ALTER TABLE ...), выполняются параллельно")
	format := flag.String("format", "binary", "формат COPY в PostgreSQL: binary | text")
	gcPercent := flag.Int("gc", 400, "GOGC для утилиты: реже сборка мусора ценой памяти")
	stateFile := flag.String("state", "onec-mssql2pg-state.json", "prepare/restore: файл с прежними настройками сервера")
	restartCmd := flag.String("restart-cmd", "", "prepare/restore: команда перезапуска PostgreSQL; без нее — выход с кодом 3")
	safe := flag.Bool("safe", false, "prepare: не выключать fsync и full_page_writes (по умолчанию выключаются: до конца переноса база не боевая)")
	mwm := flag.String("maintenance-work-mem", "2GB", "prepare: maintenance_work_mem на время заливки и индексов")
	maintWorkers := flag.Int("maint-workers", 8, "prepare: max_parallel_maintenance_workers")
	cpuProfile := flag.String("cpuprofile", "", "записать профиль процессора (pprof) в файл")
	fanin := flag.Bool("fanin", true, "copy при wal_level=minimal: части таблицы читаются параллельно и сходятся в один COPY (без WAL, с FREEZE)")
	perTable := flag.Int("readers-per-table", 8, "copy с fanin: не больше читателей на одну таблицу (один процесс PG пишет ~0,9 млн строк/с)")
	unloggedSplit := flag.Bool("unlogged-split", false, "copy: разрезанные таблицы UNLOGGED на время заливки (SET LOGGED после переписывает таблицу в один поток — обычно невыгодно)")
	phase := flag.String("phase", "all", "copy: all | pre (очистка) | load (заливка своей доли) | post (SET LOGGED) — для запуска на нескольких хостах")
	shard := flag.String("shard", "", "copy: доля задач K/N (1..N) для запуска на N хостах, например 1/2")
	nokey := flag.Bool("nokey", false, "copy: только таблицы без уникального ключа (в окне их переливают целиком — докатить нечем)")
	lockTimeout := flag.Int("lock-timeout", 2000, "cdc-enable: ожидание блокировки таблицы, мс; не дождались — таблица откладывается")
	rounds := flag.Int("rounds", 20, "cdc-enable: кругов повтора для отложенных таблиц")
	loop := flag.Int("loop", 0, "delta: повторять каждые N секунд (0 — один проход)")
	only := flag.String("only", "", "регулярное выражение: переносить только эти таблицы (имена PG)")
	exclude := flag.String("exclude", "", "регулярное выражение: не переносить эти таблицы (имена PG)")
	truncate := flag.Bool("truncate", false, "очистить целевые таблицы перед заливкой")
	ignoreCols := flag.String("ignore-cols", "", "verify-pg: колонки через запятую, которые не сравнивать (например _version)")
	yearOffset := flag.Int("year-offset", -1, "сдвиг лет дат 1С в MS SQL; -1 — взять из _YearOffset")
	flag.Parse()
	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}
	copyBinary = *format == "binary"
	debug.SetGCPercent(*gcPercent)

	ctx := context.Background()
	shardK, shardN := 0, 1
	if *shard != "" {
		if _, err := fmt.Sscanf(*shard, "%d/%d", &shardK, &shardN); err != nil || shardK < 1 || shardK > shardN {
			log.Fatalf("-shard: нужно K/N, 1 <= K <= N, получено %q", *shard)
		}
		shardK--
	}
	msDSN, pgDSN, refDSN := os.Getenv("MSSQL_DSN"), os.Getenv("PG_DSN"), os.Getenv("PG_REF_DSN")
	needMs := *mode == "copy" || *mode == "plan" || *mode == "splits" || *mode == "verify-counts" || *mode == "delta" || *mode == "cdc-enable" || *mode == "cdc-disable"
	if pgDSN == "" || (needMs && msDSN == "") || (*mode == "verify-pg" && refDSN == "") {
		log.Fatal("нужны переменные окружения MSSQL_DSN и PG_DSN (для verify-pg — PG_DSN и PG_REF_DSN)")
	}
	pgCfg, err := pgxpool.ParseConfig(pgDSN)
	if err != nil {
		log.Fatal(err)
	}
	pgCfg.MaxConns = int32(*workers + 2)
	pg, err := pgxpool.NewWithConfig(ctx, pgCfg)
	if err != nil {
		log.Fatal(err)
	}
	defer pg.Close()

	pgCols, err := loadPg(ctx, pg)
	if err != nil {
		log.Fatal("PG: ", err)
	}
	filter := func(name string) bool {
		if *only != "" && !regexp.MustCompile(*only).MatchString(name) {
			return false
		}
		if *exclude != "" && regexp.MustCompile(*exclude).MatchString(name) {
			return false
		}
		return true
	}

	if *mode == "prepare" || *mode == "restore" {
		var err error
		if *mode == "prepare" {
			err = runPrepare(ctx, pg, *stateFile, *restartCmd, *safe, *mwm, *maintWorkers)
		} else {
			err = runRestore(ctx, pg, *stateFile, *restartCmd)
		}
		if errors.Is(err, errRestart) {
			log.Print("перезапусти PostgreSQL и запусти режим еще раз (или передай -restart-cmd)")
			os.Exit(3)
		}
		if err != nil {
			log.Fatal(err)
		}
		return
	}

	if *mode == "ddl" {
		if err := runDDL(ctx, pg, *ddlFile, *workers); err != nil {
			log.Fatal(err)
		}
		return
	}

	if *mode == "verify-pg" {
		refCfg, err := pgxpool.ParseConfig(refDSN)
		if err != nil {
			log.Fatal(err)
		}
		refCfg.MaxConns = int32(*workers + 2)
		ref, err := pgxpool.NewWithConfig(ctx, refCfg)
		if err != nil {
			log.Fatal(err)
		}
		defer ref.Close()
		var names []string
		for n := range pgCols {
			if filter(n) {
				names = append(names, n)
			}
		}
		sort.Strings(names)
		var ignore []string
		if *ignoreCols != "" {
			ignore = strings.Split(*ignoreCols, ",")
		}
		if err := verifyPg(ctx, pg, ref, names, *workers, ignore); err != nil {
			log.Fatal(err)
		}
		return
	}

	db, err := sql.Open("sqlserver", msDSN)
	if err != nil {
		log.Fatal(err)
	}
	db.SetMaxOpenConns(*workers + 2)
	defer db.Close()

	msCols, counts, keys, sizes, err := loadMs(ctx, db)
	if err != nil {
		log.Fatal("MS SQL: ", err)
	}
	if *yearOffset < 0 {
		*yearOffset = 0
		if _, ok := msCols["_yearoffset"]; ok {
			if err := db.QueryRowContext(ctx, "select top 1 Offset from _YearOffset").Scan(yearOffset); err != nil && err != sql.ErrNoRows {
				log.Fatal("_YearOffset: ", err)
			}
		}
	}
	all, problems := buildTables(pgCols, msCols, counts, keys, sizes)
	for _, p := range problems {
		log.Print("структура: ", p)
	}
	var tables []*Table
	var total int64
	for _, t := range all {
		if filter(t.Name) {
			tables = append(tables, t)
			total += t.Rows
		}
	}
	if *nokey {
		keys, err := uniqueKeys(ctx, db)
		if err != nil {
			log.Fatal(err)
		}
		var nk []*Table
		for _, t := range tables {
			if len(keys[strings.ToLower(t.MsName)]) == 0 {
				nk = append(nk, t)
			}
		}
		log.Printf("-nokey: таблиц без уникального ключа %d из %d", len(nk), len(tables))
		tables = nk
		total = 0
		for _, t := range tables {
			total += t.Rows
		}
	}
	sort.Slice(tables, func(i, j int) bool { return tables[i].Rows > tables[j].Rows })
	log.Printf("таблиц к переносу: %d, строк по статистике: %d, сдвиг лет: %d", len(tables), total, *yearOffset)

	switch *mode {
	case "plan":
		for i, t := range tables {
			if i >= 30 {
				break
			}
			var ks []string
			for _, k := range t.Keys {
				ks = append(ks, k.Name)
			}
			fmt.Printf("%-40s %14d  ключ %s\n", t.Name, t.Rows, strings.Join(ks, ","))
		}
	case "splits":
		for _, t := range tables {
			parts := partsFor(t, *splitRows, *splitMB)
			tasks, err := splitTable(ctx, db, t, parts)
			if err != nil {
				log.Fatalf("%s: %v", t.Name, err)
			}
			for _, tk := range tasks {
				fmt.Printf("%s [%d/%d] ~%d: %s\n", t.Name, tk.Part, tk.Parts, tk.Est, tk.Where)
			}
		}
	case "copy":
		if err := runCopy(ctx, db, pg, tables, copyOpts{workers: *workers, splitRows: *splitRows, splitMB: *splitMB,
			yearOffset: *yearOffset, truncate: *truncate, unloggedSplit: *unloggedSplit, phase: *phase, fanin: *fanin, perTable: *perTable,
			shardK: shardK, shardN: shardN}); err != nil {
			log.Fatal(err)
		}
	case "cdc-enable":
		if err := cdcEnable(ctx, db, tables, *rounds, *lockTimeout); err != nil {
			log.Fatal(err)
		}
	case "cdc-disable":
		if err := cdcDisable(ctx, db); err != nil {
			log.Fatal(err)
		}
	case "delta":
		for {
			if err := runDelta(ctx, db, pg, tables, *workers, *yearOffset); err != nil {
				if *loop == 0 {
					log.Fatal(err)
				}
				log.Print(err)
			}
			if *loop == 0 {
				break
			}
			time.Sleep(time.Duration(*loop) * time.Second)
		}
	case "verify-counts":
		if err := verifyCounts(ctx, db, pg, tables, *workers); err != nil {
			log.Fatal(err)
		}
	default:
		log.Fatalf("неизвестный режим %q", *mode)
	}
}
