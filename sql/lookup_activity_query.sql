SELECT
    table_schema as SchemaName,
    table_name as TableName
FROM information_schema.tables
WHERE table_schema = 'public';