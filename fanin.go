package main

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Заливка «много читателей — один COPY на таблицу». Части таблицы читаются из MS SQL параллельно
// и кодируются в двоичный формат COPY, блоками целых строк; все блоки таблицы уходят в одну
// транзакцию PostgreSQL: BEGIN; TRUNCATE; COPY ... (FORMAT binary, FREEZE); COMMIT.
// При wal_level=minimal журнал для таблицы, очищенной в той же транзакции, не пишется вовсе,
// а FREEZE сразу замораживает строки — после заливки не будет массовой заморозки автоочисткой.

const faninBlock = 1 << 20 // размер блока строк, байт

// tableWriter — единственный поток записи таблицы в PostgreSQL.
type tableWriter struct {
	t       *Table
	blocks  chan []byte
	pending int // сколько частей таблицы еще не дочитано
	mu      sync.Mutex
	cancel  context.CancelFunc
	ctx     context.Context
	done    chan error
	started time.Time
}

func startTableWriter(ctx context.Context, pg *pgxpool.Pool, t *Table, parts int) *tableWriter {
	ctx, cancel := context.WithCancel(ctx)
	tw := &tableWriter{t: t, blocks: make(chan []byte, 8), pending: parts, cancel: cancel, ctx: ctx,
		done: make(chan error, 1), started: time.Now()}
	go func() { tw.done <- tw.run(pg) }()
	return tw
}

func (tw *tableWriter) run(pg *pgxpool.Pool) error {
	conn, err := pg.Acquire(tw.ctx)
	if err != nil {
		tw.cancel()
		tw.drain()
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(tw.ctx, "set synchronous_commit = off; begin; truncate "+qpg(tw.t.Name)); err != nil {
		tw.cancel()
		tw.drain()
		return fmt.Errorf("%s: truncate: %w", tw.t.Name, err)
	}
	pr, pw := io.Pipe()
	go func() {
		bw := bufio.NewWriterSize(pw, faninBlock)
		bw.Write(binHeader)
		var err error
		for b := range tw.blocks {
			if err == nil {
				_, err = bw.Write(b)
			}
		}
		if err == nil {
			bw.Write([]byte{0xff, 0xff})
			err = bw.Flush()
		}
		pw.CloseWithError(err)
	}()
	cols := make([]string, len(tw.t.Cols))
	for i, c := range tw.t.Cols {
		cols[i] = qpg(c.Name)
	}
	_, err = conn.Conn().PgConn().CopyFrom(tw.ctx, pr,
		fmt.Sprintf("copy %s (%s) from stdin with (format binary, freeze)", qpg(tw.t.Name), strings.Join(cols, ", ")))
	if err != nil {
		tw.cancel()
		pr.CloseWithError(err)
		tw.drain()
		conn.Exec(context.Background(), "rollback")
		return fmt.Errorf("%s: запись: %w", tw.t.Name, err)
	}
	if _, err := conn.Exec(tw.ctx, "commit"); err != nil {
		return fmt.Errorf("%s: commit: %w", tw.t.Name, err)
	}
	return nil
}

// drain вычитывает оставшиеся блоки, чтобы читатели не повисли на полном канале.
func (tw *tableWriter) drain() {
	go func() {
		for range tw.blocks {
		}
	}()
}

// partDone — читатель части закончил; последний закрывает поток таблицы.
func (tw *tableWriter) partDone() {
	tw.mu.Lock()
	tw.pending--
	last := tw.pending == 0
	tw.mu.Unlock()
	if last {
		close(tw.blocks)
	}
}

// readPart читает часть таблицы и отдает писателю блоки целых строк.
func readPart(db *sql.DB, tk Task, tw *tableWriter, yearOffset int, st *stats, fx *fixes) (int64, error) {
	t := tk.T
	ctx := tw.ctx
	msCols := make([]string, len(t.Cols))
	dests := make([]any, len(t.Cols))
	encs := make([]func(w wr) error, len(t.Cols))
	for i, c := range t.Cols {
		bc, err := makeBinCol(c, yearOffset, fx)
		if err != nil {
			return 0, fmt.Errorf("%s.%s: %w", t.Name, c.Name, err)
		}
		msCols[i], dests[i], encs[i] = bc.expr, bc.dest, bc.enc
	}
	q := fmt.Sprintf("select %s from %s with (nolock)", strings.Join(msCols, ", "), qms(t.MsName))
	if tk.Where != "" {
		q += " where " + tk.Where
	}
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return 0, fmt.Errorf("%s: чтение: %w", t.Name, err)
	}
	defer rows.Close()
	var nf [2]byte
	binary.BigEndian.PutUint16(nf[:], uint16(len(t.Cols)))
	buf := bytes.NewBuffer(make([]byte, 0, faninBlock+64<<10))
	var n int64
	send := func() error {
		if buf.Len() == 0 {
			return nil
		}
		b := make([]byte, buf.Len())
		copy(b, buf.Bytes())
		buf.Reset()
		select {
		case tw.blocks <- b:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for rows.Next() {
		if err := rows.Scan(dests...); err != nil {
			return n, fmt.Errorf("%s: %w", t.Name, err)
		}
		buf.Write(nf[:])
		for i, e := range encs {
			if err := e(buf); err != nil {
				return n, fmt.Errorf("%s.%s: %w", t.Name, t.Cols[i].Name, err)
			}
		}
		n++
		if n%10000 == 0 {
			st.rows.Add(10000)
		}
		if buf.Len() >= faninBlock {
			if err := send(); err != nil {
				return n, err
			}
		}
	}
	st.rows.Add(n % 10000)
	if err := rows.Err(); err != nil {
		return n, fmt.Errorf("%s: %w", t.Name, err)
	}
	return n, send()
}

// runFanin — заливка задач схемой «много читателей — один COPY на таблицу».
func runFanin(ctx context.Context, db *sql.DB, pg *pgxpool.Pool, tasks []Task, workers, perTable, yearOffset int, st *stats) []string {
	parts := map[string]int{}
	for _, tk := range tasks {
		parts[tk.T.Name]++
	}
	var mu sync.Mutex
	writers := map[string]*tableWriter{}
	var errMu sync.Mutex
	var errs []string
	addErr := func(e string) {
		errMu.Lock()
		errs = append(errs, e)
		errMu.Unlock()
		log.Print("ОШИБКА ", e)
	}
	// Итог таблицы — когда ее писатель закончил.
	var wgW sync.WaitGroup
	waitWriter := func(tw *tableWriter) {
		defer wgW.Done()
		if err := <-tw.done; err != nil {
			addErr(err.Error())
			return
		}
		if d := time.Since(tw.started); d > 10*time.Second {
			log.Printf("таблица %s записана за %s", tw.t.Name, d.Round(time.Second))
		}
	}
	// Очередь с ограничением читателей на таблицу: один процесс PG пишет таблицу примерно
	// со скоростью 0,9 млн строк/с, и лишние читатели одной таблицы только ждали бы его —
	// свободные потоки берут части других таблиц, и писателей работает несколько.
	pending := append([]Task(nil), tasks...)
	active := map[string]int{}
	cond := sync.NewCond(&mu)
	seen := map[string]bool{}
	next := func() (Task, bool) {
		mu.Lock()
		defer mu.Unlock()
		for {
			if len(pending) == 0 {
				return Task{}, false
			}
			for i, tk := range pending {
				if active[tk.T.Name] < perTable {
					pending = append(pending[:i], pending[i+1:]...)
					active[tk.T.Name]++
					if !seen[tk.T.Name] {
						seen[tk.T.Name] = true
						tw, ok := writers[tk.T.Name]
						if !ok {
							tw = startTableWriter(ctx, pg, tk.T, parts[tk.T.Name])
							writers[tk.T.Name] = tw
						}
						wgW.Add(1)
						go waitWriter(tw)
					}
					return tk, true
				}
			}
			cond.Wait()
		}
	}
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var fx fixes
			for {
				tk, ok := next()
				if !ok {
					break
				}
				mu.Lock()
				tw := writers[tk.T.Name]
				mu.Unlock()
				t0 := time.Now()
				n, err := readPart(db, tk, tw, yearOffset, st, &fx)
				if err != nil {
					tw.cancel()
					addErr(fmt.Sprintf("%s [%d/%d]: %v", tk.T.Name, tk.Part, tk.Parts, err))
				} else if tk.Parts > 1 && time.Since(t0) > 30*time.Second {
					log.Printf("прочитано %s [%d/%d]: %d строк за %s", tk.T.Name, tk.Part, tk.Parts, n, time.Since(t0).Round(time.Second))
				}
				tw.partDone()
				st.done.Add(1)
				mu.Lock()
				active[tk.T.Name]--
				cond.Broadcast()
				mu.Unlock()
			}
			st.fxMu.Lock()
			st.fx.nulChars += fx.nulChars
			st.fxMu.Unlock()
		}()
	}
	wg.Wait()
	wgW.Wait()
	return errs
}
