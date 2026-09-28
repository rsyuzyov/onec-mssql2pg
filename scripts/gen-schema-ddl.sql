-- DDL пустой копии базы 1С в MS SQL: таблицы и индексы схемы dbo из sys.* текущей базы.
-- Выполнять в контексте базы-источника с выводом в файл, дальше этот файл выполняется на сервере заготовки:
--   sqlcmd -S <источник> -d <база> -i gen-schema-ddl.sql -o ddl.sql -y 0 -f 65001
-- Таблицы CDC (схема cdc) и служебные таблицы SQL Server (is_ms_shipped) не берутся.
-- DBCC CLONEDATABASE на базе 1С падал (дубль в sys.sysschobjs) — поэтому генератор.
-- Склейка только через STRING_AGG: SELECT @v = @v + ... в SQL Server теряет строки.
SET NOCOUNT ON;
DECLARE @tables nvarchar(max) = (
SELECT STRING_AGG(CAST(N'CREATE TABLE dbo.' + QUOTENAME(t.name) + N' (' + x.cols + N');' AS nvarchar(max)), NCHAR(10)) WITHIN GROUP (ORDER BY t.name)
FROM sys.tables t
CROSS APPLY (SELECT STRING_AGG(CAST(QUOTENAME(c.name) + N' ' +
  CASE ty.name
    WHEN 'nvarchar' THEN 'nvarchar(' + CASE WHEN c.max_length = -1 THEN 'max' ELSE CAST(c.max_length/2 AS varchar) END + ')'
    WHEN 'nchar' THEN 'nchar(' + CAST(c.max_length/2 AS varchar) + ')'
    WHEN 'varchar' THEN 'varchar(' + CASE WHEN c.max_length = -1 THEN 'max' ELSE CAST(c.max_length AS varchar) END + ')'
    WHEN 'char' THEN 'char(' + CAST(c.max_length AS varchar) + ')'
    WHEN 'varbinary' THEN 'varbinary(' + CASE WHEN c.max_length = -1 THEN 'max' ELSE CAST(c.max_length AS varchar) END + ')'
    WHEN 'binary' THEN 'binary(' + CAST(c.max_length AS varchar) + ')'
    WHEN 'numeric' THEN 'numeric(' + CAST(c.precision AS varchar) + ',' + CAST(c.scale AS varchar) + ')'
    WHEN 'decimal' THEN 'decimal(' + CAST(c.precision AS varchar) + ',' + CAST(c.scale AS varchar) + ')'
    WHEN 'datetime2' THEN 'datetime2(' + CAST(c.scale AS varchar) + ')'
    ELSE ty.name END
  + CASE WHEN c.collation_name IS NOT NULL THEN N' COLLATE ' + c.collation_name ELSE N'' END
  + CASE WHEN c.is_nullable = 1 THEN N' NULL' ELSE N' NOT NULL' END AS nvarchar(max)), N', ') WITHIN GROUP (ORDER BY c.column_id) cols
  FROM sys.columns c JOIN sys.types ty ON ty.user_type_id = c.user_type_id WHERE c.object_id = t.object_id) x
WHERE t.schema_id = SCHEMA_ID('dbo') AND t.is_ms_shipped = 0);
DECLARE @indexes nvarchar(max) = (
SELECT STRING_AGG(CAST(
  CASE WHEN i.is_primary_key = 1
    THEN N'ALTER TABLE dbo.' + QUOTENAME(t.name) + N' ADD CONSTRAINT ' + QUOTENAME(i.name) + N' PRIMARY KEY ' + i.type_desc COLLATE DATABASE_DEFAULT + N' (' + k.cols + N');'
    ELSE N'CREATE ' + CASE WHEN i.is_unique = 1 THEN N'UNIQUE ' ELSE N'' END + i.type_desc COLLATE DATABASE_DEFAULT + N' INDEX ' + QUOTENAME(i.name) + N' ON dbo.' + QUOTENAME(t.name) + N' (' + k.cols + N')' + ISNULL(N' INCLUDE (' + inc.cols + N')', N'') + N';' END AS nvarchar(max)), NCHAR(10)) WITHIN GROUP (ORDER BY t.name, i.index_id)
FROM sys.tables t JOIN sys.indexes i ON i.object_id = t.object_id AND i.type IN (1, 2)
CROSS APPLY (SELECT STRING_AGG(CAST(QUOTENAME(c.name) + CASE WHEN ic.is_descending_key = 1 THEN N' DESC' ELSE N' ASC' END AS nvarchar(max)), N', ') WITHIN GROUP (ORDER BY ic.key_ordinal) cols
  FROM sys.index_columns ic JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.key_ordinal > 0) k
OUTER APPLY (SELECT STRING_AGG(CAST(QUOTENAME(c.name) AS nvarchar(max)), N', ') cols
  FROM sys.index_columns ic JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.is_included_column = 1) inc
WHERE t.schema_id = SCHEMA_ID('dbo') AND t.is_ms_shipped = 0);
SELECT @tables AS [-- tables];
SELECT N'GO' AS [-- batch];
SELECT @indexes AS [-- indexes];
SELECT N'GO' AS [-- batch];
