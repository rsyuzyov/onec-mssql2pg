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
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/microsoft/go-mssqldb"
)

func main() {
	mode := flag.String("mode", "copy", "copy | plan | verify-counts | verify-pg")
	workers := flag.Int("workers", 24, "число параллельных потоков")
	splitRows := flag.Int64("split-rows", 20_000_000, "таблицы больше этого числа строк делятся на части по ключу")
	only := flag.String("only", "", "регулярное выражение: переносить только эти таблицы (имена PG)")
	exclude := flag.String("exclude", "", "регулярное выражение: не переносить эти таблицы (имена PG)")
	truncate := flag.Bool("truncate", false, "очистить целевые таблицы перед заливкой")
	yearOffset := flag.Int("year-offset", -1, "сдвиг лет дат 1С в MS SQL; -1 — взять из _YearOffset")
	flag.Parse()

	ctx := context.Background()
	msDSN, pgDSN, refDSN := os.Getenv("MSSQL_DSN"), os.Getenv("PG_DSN"), os.Getenv("PG_REF_DSN")
	if pgDSN == "" || (*mode != "verify-pg" && msDSN == "") || (*mode == "verify-pg" && refDSN == "") {
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
		if err := verifyPg(ctx, pg, ref, names, *workers); err != nil {
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

	msCols, counts, keys, err := loadMs(ctx, db)
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
	all, problems := buildTables(pgCols, msCols, counts, keys)
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
			parts := 1
			if *splitRows > 0 && t.Rows > *splitRows {
				parts = int(t.Rows / *splitRows) + 1
			}
			tasks, err := splitTable(ctx, db, t, parts)
			if err != nil {
				log.Fatalf("%s: %v", t.Name, err)
			}
			for _, tk := range tasks {
				fmt.Printf("%s [%d/%d] ~%d: %s\n", t.Name, tk.Part, tk.Parts, tk.Est, tk.Where)
			}
		}
	case "copy":
		if err := runCopy(ctx, db, pg, tables, *workers, *splitRows, *yearOffset, *truncate); err != nil {
			log.Fatal(err)
		}
	case "verify-counts":
		if err := verifyCounts(ctx, db, pg, tables, *workers); err != nil {
			log.Fatal(err)
		}
	default:
		log.Fatalf("неизвестный режим %q", *mode)
	}
}
