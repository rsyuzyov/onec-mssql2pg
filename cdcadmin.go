package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"
)

// Включение и снятие CDC в источнике на время переноса.
//
// sp_cdc_enable_table берет блокировку схемы таблицы. Если на таблице идет долгий запрос, включение
// ждет, а новые запросы приложения встают в очередь за ним — поэтому короткий LOCK_TIMEOUT:
// таблица, которую сейчас не взять, пропускается и повторяется в следующем круге.

func cdcEnable(ctx context.Context, db *sql.DB, tables []*Table, rounds int, lockTimeoutMs int) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	var enabled bool
	if err := conn.QueryRowContext(ctx, "select is_cdc_enabled from sys.databases where name = db_name()").Scan(&enabled); err != nil {
		return err
	}
	if !enabled {
		if _, err := conn.ExecContext(ctx, "exec sys.sp_cdc_enable_db"); err != nil {
			return fmt.Errorf("sp_cdc_enable_db: %w", err)
		}
		log.Print("CDC включен в базе")
	}
	keys, err := uniqueKeys(ctx, db)
	if err != nil {
		return err
	}
	have, err := captureInstances(ctx, db)
	if err != nil {
		return err
	}
	var todo []*Table
	for _, t := range tables {
		n := strings.ToLower(t.MsName)
		if _, ok := have[n]; ok {
			continue
		}
		if len(keys[n]) == 0 {
			continue // без уникального ключа докатывать нечем — такие таблицы переливаются целиком
		}
		todo = append(todo, t)
	}
	log.Printf("включить CDC: %d таблиц (уже включено %d)", len(todo), len(have))
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("set lock_timeout %d", lockTimeoutMs)); err != nil {
		return err
	}
	start := time.Now()
	for r := 1; r <= rounds && len(todo) > 0; r++ {
		var retry []*Table
		shownErrors := 0
		for processed, t := range todo {
			_, err := conn.ExecContext(ctx, `exec sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = @p1,
				@role_name = NULL, @supports_net_changes = 0`, t.MsName)
			if err != nil {
				retry = append(retry, t)
				// Первые ошибки круга видны сразу: системная причина (например, @@SERVERNAME не равен имени машины
				// и задания агента не создаются) иначе выясняется только через все круги повтора.
				if r == rounds || shownErrors < 3 {
					log.Printf("%s: %v", t.MsName, err)
					shownErrors++
				}
			}
			if r == 1 && processed+1 == 20 && len(retry) == 20 {
				return fmt.Errorf("CDC не включился ни на одной из первых 20 таблиц — причина не в блокировках, см. ошибки выше")
			}
		}
		log.Printf("круг %d: включено %d, отложено %d (%s)", r, len(todo)-len(retry), len(retry), time.Since(start).Round(time.Second))
		todo = retry
		if len(todo) > 0 && r < rounds {
			time.Sleep(5 * time.Second)
		}
	}
	if len(todo) > 0 {
		return fmt.Errorf("CDC не включен на %d таблицах — повторить cdc-enable", len(todo))
	}
	return nil
}

// cdcDisable снимает CDC со всей базы: задания захвата и очистки, таблицы изменений.
func cdcDisable(ctx context.Context, db *sql.DB) error {
	var enabled bool
	if err := db.QueryRowContext(ctx, "select is_cdc_enabled from sys.databases where name = db_name()").Scan(&enabled); err != nil {
		return err
	}
	if !enabled {
		log.Print("CDC в базе не включен")
		return nil
	}
	if _, err := db.ExecContext(ctx, "exec sys.sp_cdc_disable_db"); err != nil {
		return fmt.Errorf("sp_cdc_disable_db: %w", err)
	}
	log.Print("CDC снят с базы")
	return nil
}
