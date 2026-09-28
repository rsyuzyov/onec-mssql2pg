# Сценарий переезда базы 1С с MS SQL на PostgreSQL

Порядок, который прошел генеральную репетицию на складской базе в ~600 ГБ (3,27 млрд строк). Времена в скобках
взяты с нее. Имена в угловых скобках замените своими:

- `<source>` — боевой SQL Server, база `<base>`;
- `<reader>` — второй хост, который читает источник и пишет в PostgreSQL (необязателен, но удваивает скорость).
  Здесь же SQL Server для заготовки и `ibcmd` новой платформы, поэтому это должен быть Windows-хост;
- `<pg>` — сервер PostgreSQL, целевая база `<target>`;
- `<app>` — сервер 1С, где база зарегистрирована.

Подключения утилиты задаются переменными `MSSQL_DSN` и `PG_DSN`. В примерах ниже `onec-mssql2pg` запускается
с ними уже заданными. Таблицы данных отбираются регуляркой:

```
EXCLUDE='^(_yearoffset|[^_].*)$'      # служебные таблицы переносит ibcmd; сюда же — таблицы, которые не нужны
```

## 0. Подготовка (за день–два)

1. **Проверить, что платформа не делает TRUNCATE.** На таблице с CDC `TRUNCATE` запрещен. Сессия
   Extended Events на `<source>` за час штатной работы: события `sql_statement_completed`, `sp_statement_completed`,
   `sql_batch_completed`, `rpc_completed` с текстом `TRUNCATE TABLE` без `#`. ⚠️ Ловите и `sql_batch_completed`:
   одного `sql_statement_completed` не хватило даже на контрольный запрос.
2. **Заморозить конфигурацию** до переезда: структура таблиц берется заранее. Пользователей замораживать не нужно,
   служебные таблицы переносятся в окне.
3. **База-структура** на `<reader>` (1–2 мин):
   ```
   .\scripts\build-struct.ps1 -SourceDb <base> -TargetPg <base>_struct -PgServer <pg> -IbcmdPath '<путь к ibcmd.exe>'
   ```
   `pg_dump --schema-only --section=post-data <base>_struct > post-data.sql` — индексы для шага 5.
4. **Целевая база** на `<pg>` (44 с): `scripts/make-target.sh create <base>_struct <target>` — таблицы без индексов
   и служебные таблицы.
5. **Проверить, что новая платформа открывает такую базу.** Удобно на копии: временный агент и RAS новой версии
   на свободных портах рядом с рабочим кластером, `rac infobase create` без создания СУБД с
   `--scheduled-jobs-deny=on`, вход через COM-коннектор новой версии, пара запросов.

## 1. День переезда, база работает

| шаг | команда | время |
| --- | --- | --- |
| PostgreSQL в режим заливки | `onec-mssql2pg -mode prepare -state state.json` (при коде 3 — перезапустить PostgreSQL и повторить) | 1 мин |
| CDC на источнике | `onec-mssql2pg -mode cdc-enable -exclude "$EXCLUDE"` | 4,5 мин на 1531 таблицу |
| снимок источника | `CREATE DATABASE <base>_snap ON (NAME = <логическое имя файла данных>, FILENAME = '...\<base>_snap.ss') AS SNAPSHOT OF [<base>]` | секунды |
| заливка из снимка | на `<reader>`: `-mode copy -phase load -shard 1/2 -workers 20 -split-rows 15000000 -split-mb 4096 -exclude "$EXCLUDE"`, на `<pg>` то же с `-shard 2/2 -workers 16`; `MSSQL_DSN` — на снимок | 23 мин |
| индексы | на `<pg>`: `-mode ddl -ddl post-data.sql -workers 10` | 26 мин |
| докатка циклом | `-mode delta -loop 120 -exclude "$EXCLUDE"`; `MSSQL_DSN` — на живую базу | 0,4 с на 43 тыс. изменений |

⚠️ `CREATE DATABASE … AS SNAPSHOT` есть в Enterprise и Developer. Снимок читается с тех же дисков, что и
боевая база: за время заливки с них будет прочитан весь объем базы.

Режим `prepare` ставит `wal_level=minimal`, `max_wal_senders=0`, выключает `fsync`, `full_page_writes` и
автоочистку, в том числе по каждой таблице. `-safe` оставляет `fsync` и `full_page_writes` включенными.

## 2. Окно (~7,5 мин без проверок приложения)

1. Запретить работу в базе: `rac infobase update --sessions-deny=on --scheduled-jobs-deny=on` (запомните прежние
   флаги — вернуть нужно их, а не `off`), дождаться ухода сеансов.
2. Последний `-mode delta` после того, как захват CDC догонит журнал.
3. Параллельно — служебные таблицы на момент запрета: `build-struct.ps1 -TargetPg <base>_win …` (1 мин 16 с) и
   `make-target.sh service <base>_win <target>` (3 с).
4. Таблицы без уникального ключа: `-mode copy -nokey -exclude "$EXCLUDE"` (5 с на 97 таблиц).
5. Сверка: `-mode verify-counts -exclude "$EXCLUDE"` (4 мин 18 с).
6. `onec-mssql2pg -mode restore -state state.json`: настройки вернутся, будут выполнены `checkpoint` и `sync` ОС (11 с).
   ⚠️ Файлы, записанные при `fsync=off`, PostgreSQL сам не досинхронизирует — поэтому `sync`.
   `vacuumdb --analyze-only --jobs=16 -d <target>` (1 мин 16 с).
7. Регистрация базы на PostgreSQL:
   `scripts/register-pg.ps1 -Infobase <base> -DbServer <pg> -DbName <target> -RacPath '<rac.exe>'`.
   Первой строкой скрипт печатает прежнюю регистрацию: это параметры отката.
8. Проверки приложения (вход, веб-сервисы, обмены), затем разрешить работу — вернуть прежние флаги.

Если вместе с СУБД меняется и версия платформы (у нас 8.3.18 → 8.5: 8.3.18 с PostgreSQL 18 не работает),
агент и RAS переключаются на новую версию между шагами 6 и 7. Модуль веб-сервера меняется на новую версию
с полным перезапуском Apache: `graceful` не выгружает старый модуль.

## 3. После

- `-mode cdc-disable` на источнике, удалить снимок, снять сессию Extended Events.
- Бэкап PostgreSQL.
- Автоочистка по таблицам включена `restore`. Статистику уже собрал `ANALYZE` в окне.

## Откат

- До шага 7 окна источник не тронут: разрешить работу, выключить CDC.
- После шага 7, пока работа не разрешена: `register-pg.ps1 -Dbms MSSQLServer` с прежними параметрами.
  Данные MS SQL не менялись, откат полный.
- После разрешения работы вернуться на MS SQL можно только с потерей изменений, сделанных в PostgreSQL:
  обратной докатки нет.
