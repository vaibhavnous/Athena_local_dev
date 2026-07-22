-- Athena pipeline DB cleanup template.
-- Dry-run by default. Set @execute = 1 to delete.

DECLARE @schema sysname = N'metadata';
DECLARE @run_id nvarchar(100) = N'PASTE_RUN_ID_HERE';
DECLARE @execute bit = 0;
DECLARE @include_ai_store bit = 1;

DECLARE @sql nvarchar(max);

SET @sql = N'
SELECT ''hitl_review_queue'' AS table_name, COUNT(*) AS rows_to_delete
FROM ' + QUOTENAME(@schema) + N'.[hitl_review_queue]
WHERE run_id = @run_id
UNION ALL
SELECT ''ai_store'', COUNT(*)
FROM ' + QUOTENAME(@schema) + N'.[ai_store]
WHERE run_id = @run_id
UNION ALL
SELECT ''kpi_checkpoints'', COUNT(*)
FROM ' + QUOTENAME(@schema) + N'.[kpi_checkpoints]
WHERE run_id = @run_id;';

EXEC sp_executesql @sql, N'@run_id nvarchar(100)', @run_id = @run_id;

IF @execute = 1
BEGIN
    BEGIN TRANSACTION;

    SET @sql = N'DELETE FROM ' + QUOTENAME(@schema) + N'.[hitl_review_queue] WHERE run_id = @run_id;';
    EXEC sp_executesql @sql, N'@run_id nvarchar(100)', @run_id = @run_id;

    IF @include_ai_store = 1
    BEGIN
        SET @sql = N'DELETE FROM ' + QUOTENAME(@schema) + N'.[ai_store] WHERE run_id = @run_id;';
        EXEC sp_executesql @sql, N'@run_id nvarchar(100)', @run_id = @run_id;
    END

    SET @sql = N'DELETE FROM ' + QUOTENAME(@schema) + N'.[kpi_checkpoints] WHERE run_id = @run_id;';
    EXEC sp_executesql @sql, N'@run_id nvarchar(100)', @run_id = @run_id;

    COMMIT TRANSACTION;
END;
