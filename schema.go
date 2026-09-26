package main

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Col — колонка целевой таблицы и ее пара в источнике.
type Col struct {
	Name   string // имя в PG (нижний регистр)
	MsName string // имя в MS SQL
	MsType string // тип в MS SQL: binary, nvarchar, datetime, numeric...
	MsLen  int    // max_length в MS SQL (для binary(1) -> bool)
	PgType string // udt_name в PG: bytea, mvarchar, mchar, bool, numeric, timestamp, int4...
}

// KeyCol — колонка кластерного ключа: имя в MS SQL и тип для CONVERT(<тип>, 0x...).
type KeyCol struct{ Name, Type string }

// Table — таблица, которую переносим.
type Table struct {
	Name      string // имя в PG
	MsName    string // имя в MS SQL
	Cols      []Col
	Rows      int64    // оценка числа строк по sys.partitions
	SizeMB    int64    // объем таблицы в MS SQL вместе с LOB
	Keys      []KeyCol // колонки кластерного индекса в MS SQL по порядку
	DataTable bool     // таблица данных 1С (имя с "_"), к ее датам применяется сдвиг лет
}

type pgCol struct{ table, name, typ string }

func loadPg(ctx context.Context, pool *pgxpool.Pool) (map[string][]pgCol, error) {
	rows, err := pool.Query(ctx, `
		select c.relname, a.attname, t.typname
		from pg_class c
		join pg_namespace n on n.oid = c.relnamespace
		join pg_attribute a on a.attrelid = c.oid and a.attnum > 0 and not a.attisdropped
		join pg_type t on t.oid = a.atttypid
		where n.nspname = 'public' and c.relkind = 'r'
		order by c.relname, a.attnum`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	res := map[string][]pgCol{}
	for rows.Next() {
		var c pgCol
		if err := rows.Scan(&c.table, &c.name, &c.typ); err != nil {
			return nil, err
		}
		res[c.table] = append(res[c.table], c)
	}
	return res, rows.Err()
}

type msCol struct {
	table, name, typ    string
	maxLen, prec, scale int
}

func loadMs(ctx context.Context, db *sql.DB) (map[string][]msCol, map[string]int64, map[string][]msCol, map[string]int64, error) {
	cols := map[string][]msCol{}
	rows, err := db.QueryContext(ctx, `
		select t.name, c.name, ty.name, c.max_length, c.precision, c.scale
		from sys.tables t
		join sys.columns c on c.object_id = t.object_id
		join sys.types ty on ty.user_type_id = c.user_type_id
		order by t.name, c.column_id`)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	for rows.Next() {
		var c msCol
		if err := rows.Scan(&c.table, &c.name, &c.typ, &c.maxLen, &c.prec, &c.scale); err != nil {
			rows.Close()
			return nil, nil, nil, nil, err
		}
		cols[strings.ToLower(c.table)] = append(cols[strings.ToLower(c.table)], c)
	}
	rows.Close()

	counts := map[string]int64{}
	rows, err = db.QueryContext(ctx, `
		select t.name, sum(p.rows)
		from sys.tables t join sys.partitions p on p.object_id = t.object_id and p.index_id in (0, 1)
		group by t.name`)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	for rows.Next() {
		var n string
		var r int64
		if err := rows.Scan(&n, &r); err != nil {
			rows.Close()
			return nil, nil, nil, nil, err
		}
		counts[strings.ToLower(n)] = r
	}
	rows.Close()

	keys := map[string][]msCol{}
	rows, err = db.QueryContext(ctx, `
		select t.name, c.name, ty.name, c.max_length, c.precision, c.scale
		from sys.tables t
		join sys.indexes i on i.object_id = t.object_id and i.type = 1
		join sys.index_columns ic on ic.object_id = i.object_id and ic.index_id = i.index_id and ic.key_ordinal > 0
		join sys.columns c on c.object_id = t.object_id and c.column_id = ic.column_id
		join sys.types ty on ty.user_type_id = c.user_type_id
		order by t.name, ic.key_ordinal`)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	for rows.Next() {
		var c msCol
		if err := rows.Scan(&c.table, &c.name, &c.typ, &c.maxLen, &c.prec, &c.scale); err != nil {
			rows.Close()
			return nil, nil, nil, nil, err
		}
		keys[strings.ToLower(c.table)] = append(keys[strings.ToLower(c.table)], c)
	}
	rows.Close()
	sizes := map[string]int64{}
	rows, err = db.QueryContext(ctx, `
		select t.name, sum(a.total_pages) * 8 / 1024
		from sys.tables t
		join sys.partitions p on p.object_id = t.object_id and p.index_id in (0, 1)
		join sys.allocation_units a on a.container_id = p.partition_id
		group by t.name`)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	for rows.Next() {
		var n string
		var mb int64
		if err := rows.Scan(&n, &mb); err != nil {
			rows.Close()
			return nil, nil, nil, nil, err
		}
		sizes[strings.ToLower(n)] = mb
	}
	rows.Close()
	return cols, counts, keys, sizes, nil
}

// msTypeDecl — объявление типа для CONVERT(<тип>, 0x...).
func msTypeDecl(c msCol) string {
	switch c.typ {
	case "binary", "varbinary", "char", "varchar":
		return fmt.Sprintf("%s(%d)", c.typ, c.maxLen)
	case "nchar", "nvarchar":
		return fmt.Sprintf("%s(%d)", c.typ, c.maxLen/2)
	case "numeric", "decimal":
		return fmt.Sprintf("%s(%d,%d)", c.typ, c.prec, c.scale)
	default:
		return c.typ
	}
}

// buildTables сопоставляет таблицы PG (они задают состав) с таблицами MS SQL.
func buildTables(pg map[string][]pgCol, ms map[string][]msCol, counts map[string]int64, keys map[string][]msCol, sizes map[string]int64) ([]*Table, []string) {
	var tables []*Table
	var problems []string
	for name, pcols := range pg {
		mcols, ok := ms[name]
		if !ok {
			problems = append(problems, fmt.Sprintf("нет в MS SQL: %s", name))
			continue
		}
		byName := map[string]msCol{}
		for _, c := range mcols {
			byName[strings.ToLower(c.name)] = c
		}
		t := &Table{Name: name, MsName: mcols[0].table, Rows: counts[name], SizeMB: sizes[name]}
		bad := false
		for _, pc := range pcols {
			mc, ok := byName[pc.name]
			if !ok {
				problems = append(problems, fmt.Sprintf("%s: колонки %s нет в MS SQL", name, pc.name))
				bad = true
				continue
			}
			t.Cols = append(t.Cols, Col{Name: pc.name, MsName: mc.name, MsType: mc.typ, MsLen: mc.maxLen, PgType: pc.typ})
		}
		if len(mcols) != len(pcols) {
			problems = append(problems, fmt.Sprintf("%s: колонок в MS SQL %d, в PG %d", name, len(mcols), len(pcols)))
		}
		if bad {
			continue
		}
		for _, k := range keys[name] {
			t.Keys = append(t.Keys, KeyCol{Name: k.name, Type: msTypeDecl(k)})
		}
		tables = append(tables, t)
	}
	for name := range ms {
		if _, ok := pg[name]; !ok {
			problems = append(problems, fmt.Sprintf("нет в PG: %s", name))
		}
	}
	sort.Strings(problems)
	return tables, problems
}
