package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Докатка изменений через Change Data Capture MS SQL.
//
// Для каждой таблицы читаются строки cdc.<экземпляр>_CT между сохраненной отметкой LSN и текущим
// максимумом, по каждому затронутому ключу (уникальный индекс в MS SQL) вычисляется итоговое
// состояние — последняя версия строки или «удалена». В PostgreSQL одной транзакцией: удалить
// затронутые ключи, вставить итоговые версии, сохранить отметку. Повтор безопасен: итоговые
// версии перезаписывают те же строки, поэтому начинать можно с момента включения CDC, даже если
// он раньше снимка, с которого залита база.

const stateSchema = "onec_mssql2pg"

type deltaTable struct {
	t      *Table
	keyIdx []int  // индексы ключевых колонок в t.Cols
	ci     string // экземпляр захвата CDC
}

func ensureState(ctx context.Context, pg *pgxpool.Pool) error {
	_, err := pg.Exec(ctx, fmt.Sprintf(`create schema if not exists %[1]s;
		create table if not exists %[1]s.lsn (tbl text primary key, lsn bytea not null, rows bigint not null default 0, at timestamptz not null default now())`, stateSchema))
	return err
}

func loadLSN(ctx context.Context, pg *pgxpool.Pool) (map[string][]byte, error) {
	rows, err := pg.Query(ctx, fmt.Sprintf("select tbl, lsn from %s.lsn", stateSchema))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	res := map[string][]byte{}
	for rows.Next() {
		var t string
		var l []byte
		if err := rows.Scan(&t, &l); err != nil {
			return nil, err
		}
		res[t] = l
	}
	return res, rows.Err()
}

// captureInstances — таблица MS SQL (нижний регистр) -> экземпляр захвата CDC.
func captureInstances(ctx context.Context, db *sql.DB) (map[string]string, error) {
	rows, err := db.QueryContext(ctx, `select object_name(source_object_id), capture_instance from cdc.change_tables`)
	if err != nil {
		return nil, fmt.Errorf("CDC в базе не включен? %w", err)
	}
	defer rows.Close()
	res := map[string]string{}
	for rows.Next() {
		var t, ci string
		if err := rows.Scan(&t, &ci); err != nil {
			return nil, err
		}
		res[strings.ToLower(t)] = ci
	}
	return res, rows.Err()
}

// uniqueKeys — колонки уникального индекса таблицы (кластерный, иначе первичный ключ, иначе первый уникальный).
func uniqueKeys(ctx context.Context, db *sql.DB) (map[string][]string, error) {
	rows, err := db.QueryContext(ctx, `
		with u as (
			select t.object_id, t.name tname, i.index_id,
				row_number() over (partition by t.object_id order by case when i.type = 1 then 0 when i.is_primary_key = 1 then 1 else 2 end, i.index_id) rn
			from sys.tables t join sys.indexes i on i.object_id = t.object_id and i.is_unique = 1 and i.has_filter = 0)
		select u.tname, c.name
		from u join sys.index_columns ic on ic.object_id = u.object_id and ic.index_id = u.index_id and ic.key_ordinal > 0
		join sys.columns c on c.object_id = ic.object_id and c.column_id = ic.column_id
		where u.rn = 1
		order by u.tname, ic.key_ordinal`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	res := map[string][]string{}
	for rows.Next() {
		var t, c string
		if err := rows.Scan(&t, &c); err != nil {
			return nil, err
		}
		res[strings.ToLower(t)] = append(res[strings.ToLower(t)], strings.ToLower(c))
	}
	return res, rows.Err()
}

type deltaResult struct {
	changes, keys int64
}

// applyTable докатывает изменения одной таблицы до toLSN.
func applyTable(ctx context.Context, db *sql.DB, pg *pgxpool.Pool, dt deltaTable, from []byte, toLSN []byte, yearOffset int) (deltaResult, error) {
	var res deltaResult
	t := dt.t
	var fx fixes
	cols := make([]*binCol, len(t.Cols))
	sel := []string{"__$start_lsn", "__$seqval", "__$operation"}
	for i, c := range t.Cols {
		bc, err := makeBinCol(c, yearOffset, &fx)
		if err != nil {
			return res, err
		}
		cols[i] = bc
		sel = append(sel, bc.expr)
	}
	cond := "__$start_lsn <= @p2"
	args := []any{toLSN}
	if from != nil {
		cond = "__$start_lsn > @p1 and " + cond
		args = []any{from, toLSN}
	} else {
		args = []any{nil, toLSN}
	}
	q := fmt.Sprintf("select %s from cdc.%s with (nolock) where %s order by __$start_lsn, __$seqval, __$operation",
		strings.Join(sel, ", "), qms(dt.ci+"_CT"), cond)
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return res, fmt.Errorf("%s: чтение CDC: %w", t.Name, err)
	}
	defer rows.Close()

	type final struct {
		key []byte // кортеж ключа в формате двоичного COPY
		img []byte // кортеж строки; nil — строки нет
	}
	finals := map[string]*final{}
	var order []string
	var lsn, seq []byte
	var op int64
	dests := append([]any{&lsn, &seq, &op}, make([]any, len(cols))...)
	for i, bc := range cols {
		dests[3+i] = bc.dest
	}
	var nf, nk [2]byte
	binary.BigEndian.PutUint16(nf[:], uint16(len(cols)))
	binary.BigEndian.PutUint16(nk[:], uint16(len(dt.keyIdx)))
	var kb, rb bytes.Buffer
	for rows.Next() {
		if err := rows.Scan(dests...); err != nil {
			return res, fmt.Errorf("%s: %w", t.Name, err)
		}
		res.changes++
		kb.Reset()
		kb.Write(nk[:])
		for _, i := range dt.keyIdx {
			if err := cols[i].enc(&kb); err != nil {
				return res, fmt.Errorf("%s.%s: %w", t.Name, t.Cols[i].Name, err)
			}
		}
		k := string(kb.Bytes())
		f, ok := finals[k]
		if !ok {
			f = &final{key: append([]byte(nil), kb.Bytes()...)}
			finals[k] = f
			order = append(order, k)
		}
		switch op {
		case 1, 3: // удаление, образ до изменения
			f.img = nil
		case 2, 4: // вставка, образ после изменения
			rb.Reset()
			rb.Write(nf[:])
			for i, bc := range cols {
				if err := bc.enc(&rb); err != nil {
					return res, fmt.Errorf("%s.%s: %w", t.Name, t.Cols[i].Name, err)
				}
			}
			f.img = append([]byte(nil), rb.Bytes()...)
		}
	}
	if err := rows.Err(); err != nil {
		return res, fmt.Errorf("%s: %w", t.Name, err)
	}
	res.keys = int64(len(finals))

	conn, err := pg.Acquire(ctx)
	if err != nil {
		return res, err
	}
	defer conn.Release()
	tx, err := conn.Begin(ctx)
	if err != nil {
		return res, err
	}
	defer tx.Rollback(context.Background())
	if len(finals) > 0 {
		var keyNames, allNames, on []string
		for _, i := range dt.keyIdx {
			n := qpg(t.Cols[i].Name)
			keyNames = append(keyNames, n)
			on = append(on, fmt.Sprintf("t.%s = k.%s", n, n))
		}
		for _, c := range t.Cols {
			allNames = append(allNames, qpg(c.Name))
		}
		if _, err := tx.Exec(ctx, fmt.Sprintf(`create temp table _k on commit drop as select %s from %s limit 0;
			create temp table _r (like %s) on commit drop`, strings.Join(keyNames, ", "), qpg(t.Name), qpg(t.Name))); err != nil {
			return res, fmt.Errorf("%s: %w", t.Name, err)
		}
		var kbuf, rbuf bytes.Buffer
		kbuf.Write(binHeader)
		rbuf.Write(binHeader)
		for _, k := range order {
			f := finals[k]
			kbuf.Write(f.key)
			if f.img != nil {
				rbuf.Write(f.img)
			}
		}
		kbuf.Write([]byte{0xff, 0xff})
		rbuf.Write([]byte{0xff, 0xff})
		if _, err := tx.Conn().PgConn().CopyFrom(ctx, &kbuf, fmt.Sprintf("copy _k (%s) from stdin with (format binary)", strings.Join(keyNames, ", "))); err != nil {
			return res, fmt.Errorf("%s: ключи: %w", t.Name, err)
		}
		if _, err := tx.Conn().PgConn().CopyFrom(ctx, &rbuf, fmt.Sprintf("copy _r (%s) from stdin with (format binary)", strings.Join(allNames, ", "))); err != nil {
			return res, fmt.Errorf("%s: строки: %w", t.Name, err)
		}
		if _, err := tx.Exec(ctx, fmt.Sprintf("delete from %s t using _k k where %s", qpg(t.Name), strings.Join(on, " and "))); err != nil {
			return res, fmt.Errorf("%s: delete: %w", t.Name, err)
		}
		if _, err := tx.Exec(ctx, fmt.Sprintf("insert into %s (%s) select %s from _r", qpg(t.Name), strings.Join(allNames, ", "), strings.Join(allNames, ", "))); err != nil {
			return res, fmt.Errorf("%s: insert: %w", t.Name, err)
		}
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf(`insert into %s.lsn (tbl, lsn, rows, at) values ($1, $2, $3, now())
		on conflict (tbl) do update set lsn = excluded.lsn, rows = %[1]s.lsn.rows + excluded.rows, at = now()`, stateSchema),
		t.Name, toLSN, res.changes); err != nil {
		return res, err
	}
	return res, tx.Commit(ctx)
}

// runDelta — один проход докатки по всем таблицам с CDC.
func runDelta(ctx context.Context, db *sql.DB, pg *pgxpool.Pool, tables []*Table, workers, yearOffset int) error {
	if err := ensureState(ctx, pg); err != nil {
		return err
	}
	cis, err := captureInstances(ctx, db)
	if err != nil {
		return err
	}
	keys, err := uniqueKeys(ctx, db)
	if err != nil {
		return err
	}
	var list []deltaTable
	var noKey, noCDC []string
	for _, t := range tables {
		ci, ok := cis[strings.ToLower(t.MsName)]
		if !ok {
			noCDC = append(noCDC, t.Name)
			continue
		}
		kc := keys[strings.ToLower(t.MsName)]
		if len(kc) == 0 {
			noKey = append(noKey, t.Name)
			continue
		}
		dt := deltaTable{t: t, ci: ci}
		for _, k := range kc {
			for i, c := range t.Cols {
				if strings.ToLower(c.MsName) == k {
					dt.keyIdx = append(dt.keyIdx, i)
				}
			}
		}
		if len(dt.keyIdx) != len(kc) {
			noKey = append(noKey, t.Name)
			continue
		}
		list = append(list, dt)
	}
	if len(noCDC) > 0 {
		log.Printf("без CDC (не докатываются): %d таблиц, например %s", len(noCDC), strings.Join(noCDC[:min(5, len(noCDC))], ", "))
	}
	if len(noKey) > 0 {
		log.Printf("без уникального ключа (переливать целиком): %d таблиц, например %s", len(noKey), strings.Join(noKey[:min(5, len(noKey))], ", "))
	}
	var toLSN []byte
	if err := db.QueryRowContext(ctx, "select sys.fn_cdc_get_max_lsn()").Scan(&toLSN); err != nil {
		return err
	}
	lsns, err := loadLSN(ctx, pg)
	if err != nil {
		return err
	}
	start := time.Now()
	sort.Slice(list, func(i, j int) bool { return list[i].t.Rows > list[j].t.Rows })
	ch := make(chan deltaTable)
	var mu sync.Mutex
	var total deltaResult
	var errs []string
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for dt := range ch {
				r, err := applyTable(ctx, db, pg, dt, lsns[dt.t.Name], toLSN, yearOffset)
				mu.Lock()
				if err != nil {
					errs = append(errs, err.Error())
					log.Printf("ОШИБКА %v", err)
				} else {
					total.changes += r.changes
					total.keys += r.keys
					if r.changes > 0 {
						log.Printf("докатка %s: изменений %d, ключей %d", dt.t.Name, r.changes, r.keys)
					}
				}
				mu.Unlock()
			}
		}()
	}
	for _, dt := range list {
		ch <- dt
	}
	close(ch)
	wg.Wait()
	log.Printf("докатка до LSN %s: таблиц %d, изменений %d, ключей %d за %s", hex.EncodeToString(toLSN), len(list), total.changes, total.keys, time.Since(start).Round(time.Millisecond))
	if len(errs) > 0 {
		return fmt.Errorf("ошибок %d, первая: %s", len(errs), errs[0])
	}
	return nil
}
