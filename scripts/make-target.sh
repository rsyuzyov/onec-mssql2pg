#!/bin/bash
# usage: make-target.sh create|service <база-структура> <целевая база> — от root на PG-хосте переноса
#   create  — целевая база с нуля: CREATE DATABASE как у базы-структуры, pre-data (таблицы без индексов), служебные таблицы;
#   service — служебные таблицы целевой базы (все, кроме ^_) заменить таблицами базы-структуры одной транзакцией.
# База-структура — пустая база, построенная ibcmd по боевой (build-struct.ps1): служебные таблицы
# в формате новой платформы берутся только из нее. Индексы — потом, утилитой: -mode ddl -ddl post-data.sql.
# Владелец объектов — postgres (pg_dump -O): ibcmd мог строить базу-структуру под другой ролью.
set -euo pipefail
mode=$1; structDb=$2; targetDb=$3
asPostgres() { runuser -u postgres -- "$@"; }
serviceTables=$(asPostgres psql -X -At -d "$structDb" -c \
  "select string_agg(format('public.%I', relname), ' ' order by relname) from pg_class c join pg_namespace n on n.oid = c.relnamespace
   where n.nspname = 'public' and c.relkind = 'r' and relname !~ '^_'")
[ -n "$serviceTables" ] || { echo "в $structDb нет служебных таблиц" >&2; exit 1; }
dumpTableArgs=()
for tableName in $serviceTables; do dumpTableArgs+=(-t "$tableName"); done

case $mode in
create)
  asPostgres createdb -T template0 -E UTF8 --locale-provider=libc --locale=ru_RU.UTF-8 "$targetDb"
  asPostgres pg_dump -s -O -x --section=pre-data -d "$structDb" | asPostgres psql -X -q -v ON_ERROR_STOP=1 -d "$targetDb"
  ;;
service) ;;
*) echo "режим: create | service" >&2; exit 2 ;;
esac

{ echo "TRUNCATE ${serviceTables// /, };"; asPostgres pg_dump -a -O -x "${dumpTableArgs[@]}" -d "$structDb"; } |
  asPostgres psql -X -q -v ON_ERROR_STOP=1 --single-transaction -d "$targetDb"
asPostgres psql -X -At -d "$targetDb" -c \
  "select 'служебные таблицы: ' || count(*) || ', v8users: ' || (select count(*) from v8users) || ', config: ' || (select count(*) from config)
   from pg_class c join pg_namespace n on n.oid = c.relnamespace where n.nspname = 'public' and c.relkind = 'r' and relname !~ '^_'"
