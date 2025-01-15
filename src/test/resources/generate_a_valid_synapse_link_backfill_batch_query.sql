INSERT OVERWRITE test.table_a
SELECT * FROM (
 SELECT * FROM test.staged_a ORDER BY ROW_NUMBER() OVER (PARTITION BY Id ORDER BY versionnumber DESC) FETCH FIRST 1 ROWS WITH TIES
) WHERE coalesce(IsDelete, false) = false
