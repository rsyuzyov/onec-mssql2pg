package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sort"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
)

type tableSum struct {
	rows int64
	hash string
}

// pgTableSum — число строк и не зависящая от порядка сумма хешей строк таблицы.
func pgTableSum(ctx context.Context, pool *pgxpool.Pool, table string) (tableSum, error) {
	var s tableSum
	var h *string
	err := pool.QueryRow(ctx, fmt.Sprintf(
		"select count(*), sum(hashtextextended(t::text, 0)::numeric)::text from %s t", qpg(table))).Scan(&s.rows, &h)
	if h != nil {
		s.hash = *h
	}
	return s, err
}

// verifyPg сравнивает построчно две базы PG (перенос против эталона) по всем таблицам.
func verifyPg(ctx context.Context, a, b *pgxpool.Pool, tables []string, workers int) error {
	type res struct {
		name   string
		sa, sb tableSum
		err    error
	}
	ch := make(chan string)
	out := make(chan res)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for t := range ch {
				r := res{name: t}
				r.sa, r.err = pgTableSum(ctx, a, t)
				if r.err == nil {
					r.sb, r.err = pgTableSum(ctx, b, t)
				}
				out <- r
			}
		}()
	}
	go func() {
		for _, t := range tables {
			ch <- t
		}
		close(ch)
		wg.Wait()
		close(out)
	}()
	var diff []string
	ok := 0
	for r := range out {
		switch {
		case r.err != nil:
			diff = append(diff, fmt.Sprintf("%s: ошибка: %v", r.name, r.err))
		case r.sa != r.sb:
			diff = append(diff, fmt.Sprintf("%s: строк %d / %d, хеш совпал: %v", r.name, r.sa.rows, r.sb.rows, r.sa.hash == r.sb.hash))
		default:
			ok++
		}
	}
	sort.Strings(diff)
	for _, d := range diff {
		log.Print("РАСХОЖДЕНИЕ ", d)
	}
	log.Printf("сверка: совпало %d таблиц, расходится %d", ok, len(diff))
	if len(diff) > 0 {
		return fmt.Errorf("расхождений: %d", len(diff))
	}
	return nil
}

// verifyCounts сравнивает точное число строк в MS SQL и PG.
func verifyCounts(ctx context.Context, db *sql.DB, pg *pgxpool.Pool, tables []*Table, workers int) error {
	ch := make(chan *Table)
	var mu sync.Mutex
	var diff []string
	ok := 0
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for t := range ch {
				var nm, np int64
				err := db.QueryRowContext(ctx, "select count_big(*) from "+qms(t.MsName)+" with (nolock)").Scan(&nm)
				if err == nil {
					err = pg.QueryRow(ctx, "select count(*) from "+qpg(t.Name)).Scan(&np)
				}
				mu.Lock()
				switch {
				case err != nil:
					diff = append(diff, fmt.Sprintf("%s: ошибка: %v", t.Name, err))
				case nm != np:
					diff = append(diff, fmt.Sprintf("%s: MS SQL %d, PG %d", t.Name, nm, np))
				default:
					ok++
				}
				mu.Unlock()
			}
		}()
	}
	for _, t := range tables {
		ch <- t
	}
	close(ch)
	wg.Wait()
	sort.Strings(diff)
	for _, d := range diff {
		log.Print("РАСХОЖДЕНИЕ ", d)
	}
	log.Printf("сверка числа строк: совпало %d таблиц, расходится %d", ok, len(diff))
	if len(diff) > 0 {
		return fmt.Errorf("расхождений: %d", len(diff))
	}
	return nil
}
