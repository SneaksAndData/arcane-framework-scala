CREATE OR REPLACE TABLE test.table_a WITH (partitioning=ARRAY[], format='PARQUET', sorted_by=ARRAY[], parquet_bloom_filter_columns=ARRAY[]) AS
SELECT * FROM (
 SELECT * FROM test.staged_a ORDER BY ROW_NUMBER() OVER (PARTITION BY Id ORDER BY versionnumber DESC) FETCH FIRST 1 ROWS WITH TIES
) WHERE coalesce(IsDelete, false) = false
