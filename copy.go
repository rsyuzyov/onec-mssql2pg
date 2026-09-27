package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Task — таблица целиком или диапазон ее ведущего ключа.
type Task struct {
	T     *Table
	Where string // условие на ключ, пусто — вся таблица
	Part  int
	Solo  bool // wal_level=minimal: TRUNCATE и COPY в одной транзакции — без WAL
	Parts int
	Est   int64
}

func qms(name string) string { return "[" + strings.ReplaceAll(name, "]", "]]") + "]" }
func qpg(name string) string { return `"` + strings.ReplaceAll(name, `"`, `""`) + `"` }

// splitTable режет большую таблицу на части по кластерному ключу. Колонки ключа с единственным
// значением (разделитель, тип составного поля) уходят в условие равенства; колонка с немногими
// значениями делится по значениям со спуском к следующей колонке; колонка с многими значениями
// режется диапазонами по квантилям выборки TABLESAMPLE.
func splitTable(ctx context.Context, db *sql.DB, t *Table, parts int) ([]Task, error) {
	single := []Task{{T: t, Part: 1, Parts: 1, Est: t.Rows}}
	if parts <= 1 || len(t.Keys) == 0 {
		return single, nil
	}
	pct := 200000.0 * 100 / float64(t.Rows+1)
	if pct > 100 {
		pct = 100
	}
	if pct < 0.001 {
		pct = 0.001
	}
	var conds []string
	if err := splitRange(ctx, db, t, nil, 0, parts, pct, &conds); err != nil {
		return nil, err
	}
	if len(conds) <= 1 {
		return single, nil
	}
	tasks := make([]Task, len(conds))
	for i, c := range conds {
		tasks[i] = Task{T: t, Where: c, Part: i + 1, Parts: len(conds), Est: t.Rows / int64(len(conds))}
	}
	return tasks, nil
}

func and(c []string) string { return strings.Join(c, " and ") }

// splitRange добавляет в out непересекающиеся условия, покрывающие строки с условием prefix.
func splitRange(ctx context.Context, db *sql.DB, t *Table, prefix []string, ki, parts int, pct float64, out *[]string) error {
	whole := func() { *out = append(*out, and(prefix)) }
	if parts <= 1 || ki >= len(t.Keys) {
		whole()
		return nil
	}
	k := t.Keys[ki]
	col := qms(k.Name)
	lit := func(b []byte) string { return fmt.Sprintf("convert(%s, 0x%s)", k.Type, hex.EncodeToString(b)) }
	with := func(extra ...string) []string { return append(append([]string{}, prefix...), extra...) }
	where := ""
	if len(prefix) > 0 {
		where = " where " + and(prefix)
	}
	rows, err := db.QueryContext(ctx, fmt.Sprintf(
		"select convert(varbinary(900), x.c) from (select %s c from %s tablesample system (%.4f percent) repeatable (1541) with (nolock)%s) x where x.c is not null order by x.c",
		col, qms(t.MsName), pct, where))
	if err != nil {
		return err
	}
	var sample [][]byte
	for rows.Next() {
		var b []byte
		if err := rows.Scan(&b); err != nil {
			rows.Close()
			return err
		}
		sample = append(sample, b)
	}
	rows.Close()
	if len(sample) < 100 {
		whole()
		return nil
	}
	// Частоты значений в выборке (она отсортирована).
	type grp struct {
		v []byte
		n int
	}
	var groups []grp
	for _, b := range sample {
		if len(groups) > 0 && string(groups[len(groups)-1].v) == string(b) {
			groups[len(groups)-1].n++
		} else {
			groups = append(groups, grp{b, 1})
		}
	}
	if len(groups) <= 64 {
		// Немного значений: по каждому — свое равенство и спуск к следующей колонке ключа.
		var vals []string
		for _, g := range groups {
			sub := int(float64(parts)*float64(g.n)/float64(len(sample)) + 0.5)
			if sub < 1 {
				sub = 1
			}
			vals = append(vals, lit(g.v))
			if err := splitRange(ctx, db, t, with(fmt.Sprintf("%s = %s", col, lit(g.v))), ki+1, sub, pct, out); err != nil {
				return err
			}
		}
		// Значения, не попавшие в выборку, и NULL — отдельной частью.
		*out = append(*out, and(with(fmt.Sprintf("(%s not in (%s) or %s is null)", col, strings.Join(vals, ", "), col))))
		return nil
	}
	var bounds [][]byte
	for i := 1; i < parts; i++ {
		b := sample[len(sample)*i/parts]
		if len(bounds) > 0 && string(bounds[len(bounds)-1]) == string(b) {
			continue
		}
		bounds = append(bounds, b)
	}
	for i := 0; i <= len(bounds); i++ {
		var c []string
		if i > 0 {
			c = append(c, fmt.Sprintf("%s > %s", col, lit(bounds[i-1])))
		}
		if i < len(bounds) {
			c = append(c, fmt.Sprintf("%s <= %s", col, lit(bounds[i])))
		}
		if i == 0 {
			c = []string{"(" + and(c) + " or " + col + " is null)"}
		}
		*out = append(*out, and(with(c...)))
	}
	return nil
}

type stats struct {
	rows  atomic.Int64
	fx    fixes
	fxMu  sync.Mutex
	done  atomic.Int64
	total int64
}

// copyBinary — писать в PG двоичным COPY (по умолчанию) или текстовым.
var copyBinary = true

// binHeader: PGCOPY, 0x0a 0xff 0x0d 0x0a 0x00, flags int32, header extension length int32.
var binHeader = []byte{'P', 'G', 'C', 'O', 'P', 'Y', 0x0a, 0xff, 0x0d, 0x0a, 0x00, 0, 0, 0, 0, 0, 0, 0, 0}

func runTask(ctx context.Context, db *sql.DB, pg *pgxpool.Pool, tk Task, yearOffset int, st *stats) (int64, error) {
	t := tk.T
	off := yearOffset // сдвиг лет есть и в служебных таблицах (config, configcas...)
	var fx fixes
	msCols := make([]string, len(t.Cols))
	pgCols := make([]string, len(t.Cols))
	dests := make([]any, len(t.Cols))
	var encs []encoder
	var bencs []func(w wr) error
	for i, c := range t.Cols {
		pgCols[i] = qpg(c.Name)
		if copyBinary {
			bc, err := makeBinCol(c, off, &fx)
			if err != nil {
				return 0, fmt.Errorf("%s.%s: %w", t.Name, c.Name, err)
			}
			msCols[i], dests[i] = bc.expr, bc.dest
			bencs = append(bencs, bc.enc)
			continue
		}
		e, err := makeEncoder(c, off, &fx)
		if err != nil {
			return 0, err
		}
		encs = append(encs, e)
		msCols[i] = qms(c.MsName)
	}
	vals := make([]any, len(t.Cols))
	if !copyBinary {
		for i := range vals {
			dests[i] = &vals[i]
		}
	}
	q := fmt.Sprintf("select %s from %s with (nolock)", strings.Join(msCols, ", "), qms(t.MsName))
	if tk.Where != "" {
		q += " where " + tk.Where
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return 0, fmt.Errorf("%s: чтение: %w", t.Name, err)
	}
	defer rows.Close()

	pr, pw := io.Pipe()
	var n int64
	go func() {
		bw := bufio.NewWriterSize(pw, 1<<20)
		var err error
		if copyBinary {
			bw.Write(binHeader)
		}
		var nf [2]byte
		binary.BigEndian.PutUint16(nf[:], uint16(len(t.Cols)))
		for rows.Next() {
			if err = rows.Scan(dests...); err != nil {
				break
			}
			if copyBinary {
				bw.Write(nf[:])
				for i, e := range bencs {
					if err = e(bw); err != nil {
						err = fmt.Errorf("%s.%s: %w", t.Name, t.Cols[i].Name, err)
						break
					}
				}
			} else {
				for i, e := range encs {
					if i > 0 {
						bw.WriteByte('\t')
					}
					if err = e(bw, vals[i]); err != nil {
						err = fmt.Errorf("%s.%s: %w", t.Name, t.Cols[i].Name, err)
						break
					}
				}
				bw.WriteByte('\n')
			}
			if err != nil {
				break
			}
			n++
			if n%10000 == 0 {
				st.rows.Add(10000)
			}
		}
		if err == nil {
			err = rows.Err()
		}
		st.rows.Add(n % 10000)
		if err == nil && copyBinary {
			bw.Write([]byte{0xff, 0xff})
		}
		if err == nil {
			err = bw.Flush()
		}
		pw.CloseWithError(err)
	}()

	conn, err := pg.Acquire(ctx)
	if err != nil {
		pr.CloseWithError(err)
		return 0, err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, "set synchronous_commit = off"); err != nil {
		pr.CloseWithError(err)
		return 0, err
	}
	cmd := fmt.Sprintf("copy %s (%s) from stdin", qpg(t.Name), strings.Join(pgCols, ", "))
	if copyBinary {
		cmd += " with (format binary)"
	}
	if tk.Solo {
		if _, err := conn.Exec(ctx, "begin; truncate "+qpg(t.Name)); err != nil {
			pr.CloseWithError(err)
			return 0, fmt.Errorf("%s: truncate: %w", t.Name, err)
		}
	}
	_, err = conn.Conn().PgConn().CopyFrom(ctx, pr, cmd)
	if err != nil {
		cancel()
		pr.CloseWithError(err)
		if tk.Solo {
			conn.Exec(context.Background(), "rollback")
		}
		return n, fmt.Errorf("%s: запись: %w", t.Name, err)
	}
	if tk.Solo {
		if _, err := conn.Exec(ctx, "commit"); err != nil {
			return n, fmt.Errorf("%s: commit: %w", t.Name, err)
		}
	}
	st.fxMu.Lock()
	st.fx.nulChars += fx.nulChars
	st.fxMu.Unlock()
	return n, nil
}

// partsFor — на сколько частей резать таблицу: по числу строк и по объему, берется большее.
func partsFor(t *Table, splitRows, splitMB int64) int {
	parts := 1
	if splitRows > 0 && t.Rows > splitRows {
		parts = int(t.Rows/splitRows) + 1
	}
	if splitMB > 0 && t.SizeMB > splitMB {
		if p := int(t.SizeMB/splitMB) + 1; p > parts {
			parts = p
		}
	}
	if parts > 64 {
		parts = 64
	}
	return parts
}

// copyOpts — параметры заливки.
type copyOpts struct {
	workers        int
	splitRows      int64
	splitMB        int64
	yearOffset     int
	truncate       bool
	unloggedSplit  bool   // разрезанные таблицы — UNLOGGED на время заливки (SET LOGGED после — перезапись в один поток)
	phase          string // all | pre | load | post
	perTable       int    // fanin: не больше читателей на одну таблицу
	fanin          bool   // wal_level=minimal: много читателей — один COPY на таблицу, без WAL
	shardK, shardN int    // заливка на нескольких хостах: этот экземпляр берет задачи с номером i%N == K
}

func runCopy(ctx context.Context, db *sql.DB, pg *pgxpool.Pool, tables []*Table, o copyOpts) error {
	workers, splitRows, splitMB, yearOffset, truncate := o.workers, o.splitRows, o.splitMB, o.yearOffset, o.truncate
	var tasks []Task
	for _, t := range tables {
		parts := partsFor(t, splitRows, splitMB)
		tt, err := splitTable(ctx, db, t, parts)
		if err != nil {
			return fmt.Errorf("%s: разбиение: %w", t.Name, err)
		}
		// Оценка задачи — доля объема таблицы (с LOB), а не строк: иначе таблица с крупными
		// двоичными данными уходит в хвост очереди и держит всю заливку.
		for i := range tt {
			tt[i].Est = (t.SizeMB*1024*1024 + t.Rows*100) / int64(len(tt))
		}
		tasks = append(tasks, tt...)
	}
	// Порядок задач одинаков у всех экземпляров (выборка для границ — REPEATABLE, копия неизменна):
	// на этом держится деление задач между хостами.
	sort.SliceStable(tasks, func(i, j int) bool {
		a, b := tasks[i], tasks[j]
		if a.Est != b.Est {
			return a.Est > b.Est
		}
		if a.T.Name != b.T.Name {
			return a.T.Name < b.T.Name
		}
		return a.Part < b.Part
	})

	// При wal_level=minimal журнал не пишется для таблицы, очищенной в той же транзакции (цельные
	// таблицы), и для UNLOGGED (разрезанные: их части льются параллельно, после — SET LOGGED).
	var walLevel string
	if err := pg.QueryRow(ctx, "select current_setting('wal_level')").Scan(&walLevel); err != nil {
		return err
	}
	var unlogged, split []string
	for i := range tasks {
		if tasks[i].Parts == 1 {
			tasks[i].Solo = walLevel == "minimal"
		} else if tasks[i].Part == 1 {
			split = append(split, tasks[i].T.Name)
		}
	}
	if walLevel == "minimal" && o.unloggedSplit {
		unlogged = split
	}
	pre, load, post := o.phase == "all" || o.phase == "pre", o.phase == "all" || o.phase == "load", o.phase == "all" || o.phase == "post"
	if pre {
		// Цельные таблицы очищает сама задача (TRUNCATE в ее транзакции), разрезанные — здесь, один раз.
		if truncate {
			for _, t := range tables {
				if _, err := pg.Exec(ctx, "truncate "+qpg(t.Name)); err != nil {
					return fmt.Errorf("%s: truncate: %w", t.Name, err)
				}
			}
		}
		for _, t := range unlogged {
			if _, err := pg.Exec(ctx, "alter table "+qpg(t)+" set unlogged"); err != nil {
				return fmt.Errorf("%s: set unlogged: %w", t, err)
			}
		}
	}
	if walLevel == "minimal" {
		log.Printf("wal_level=minimal: без журнала цельные таблицы (%d задач); разрезанных %d, из них UNLOGGED %d",
			len(tasks)-countParts(tasks, split), len(split), len(unlogged))
	} else {
		log.Printf("wal_level=%s: журнал пишется (ускорить — режим prepare)", walLevel)
	}
	useFanin := o.fanin && walLevel == "minimal" && copyBinary && !o.unloggedSplit
	if o.shardN > 1 {
		var mine []Task
		if useFanin {
			// У таблицы один писатель — делим по таблицам: крупные по очереди туда, где меньше строк.
			total := map[string]int64{}
			var names []string
			for _, tk := range tasks {
				if _, ok := total[tk.T.Name]; !ok {
					names = append(names, tk.T.Name)
				}
				total[tk.T.Name] += tk.Est
			}
			sort.SliceStable(names, func(i, j int) bool {
				if total[names[i]] != total[names[j]] {
					return total[names[i]] > total[names[j]]
				}
				return names[i] < names[j]
			})
			load := make([]int64, o.shardN)
			owner := map[string]int{}
			for _, n := range names {
				k := 0
				for i := range load {
					if load[i] < load[k] {
						k = i
					}
				}
				owner[n] = k
				load[k] += total[n] + 1
			}
			for _, tk := range tasks {
				if owner[tk.T.Name] == o.shardK {
					mine = append(mine, tk)
				}
			}
		} else {
			for i, tk := range tasks {
				if i%o.shardN == o.shardK {
					mine = append(mine, tk)
				}
			}
		}
		log.Printf("экземпляр %d из %d: задач %d из %d", o.shardK+1, o.shardN, len(mine), len(tasks))
		tasks = mine
	}
	if !load {
		tasks = nil
	}
	if !post {
		unlogged = nil
	}

	st := &stats{total: int64(len(tasks))}
	start := time.Now()
	stop := make(chan struct{})
	go func() {
		tick := time.NewTicker(30 * time.Second)
		defer tick.Stop()
		var last int64
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				r := st.rows.Load()
				log.Printf("прогресс: задач %d/%d, строк %d, %.0f строк/с за 30 с", st.done.Load(), st.total, r, float64(r-last)/30)
				last = r
			}
		}
	}()

	var errs []string
	if useFanin {
		log.Printf("схема «много читателей — один COPY на таблицу»: без журнала и с FREEZE")
		errs = runFanin(ctx, db, pg, tasks, workers, o.perTable, yearOffset, st)
	} else {
		ch := make(chan Task)
		var wg sync.WaitGroup
		var errMu sync.Mutex
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for tk := range ch {
					t0 := time.Now()
					n, err := runTask(ctx, db, pg, tk, yearOffset, st)
					st.done.Add(1)
					if err != nil {
						errMu.Lock()
						errs = append(errs, err.Error())
						errMu.Unlock()
						log.Printf("ОШИБКА %s [%d/%d]: %v", tk.T.Name, tk.Part, tk.Parts, err)
						continue
					}
					if d := time.Since(t0); d > 10*time.Second || tk.Parts > 1 {
						log.Printf("готово %s [%d/%d]: %d строк за %s", tk.T.Name, tk.Part, tk.Parts, n, d.Round(time.Second))
					}
				}
			}()
		}
		for _, tk := range tasks {
			ch <- tk
		}
		close(ch)
		wg.Wait()
	}
	close(stop)
	if len(unlogged) > 0 {
		t0 := time.Now()
		if err := parallelExec(ctx, pg, unlogged, workers, func(t string) string { return "alter table " + qpg(t) + " set logged" }); err != nil {
			errs = append(errs, err.Error())
		}
		log.Printf("SET LOGGED для %d таблиц за %s", len(unlogged), time.Since(t0).Round(time.Second))
	}
	log.Printf("итого: задач %d, строк %d за %s; удалено символов U+0000: %d",
		st.total, st.rows.Load(), time.Since(start).Round(time.Second), st.fx.nulChars)
	if len(errs) > 0 {
		return fmt.Errorf("ошибок: %d, первая: %s", len(errs), errs[0])
	}
	return nil
}

func countParts(tasks []Task, tables []string) int {
	set := map[string]bool{}
	for _, t := range tables {
		set[t] = true
	}
	n := 0
	for _, tk := range tasks {
		if set[tk.T.Name] {
			n++
		}
	}
	return n
}

// parallelExec выполняет по команде на таблицу в workers потоков.
func parallelExec(ctx context.Context, pg *pgxpool.Pool, tables []string, workers int, stmt func(string) string) error {
	ch := make(chan string)
	var mu sync.Mutex
	var errs []string
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for t := range ch {
				if _, err := pg.Exec(ctx, stmt(t)); err != nil {
					mu.Lock()
					errs = append(errs, fmt.Sprintf("%s: %v", t, err))
					mu.Unlock()
				}
			}
		}()
	}
	for _, t := range tables {
		ch <- t
	}
	close(ch)
	wg.Wait()
	if len(errs) > 0 {
		return fmt.Errorf("ошибок %d, первая: %s", len(errs), errs[0])
	}
	return nil
}
