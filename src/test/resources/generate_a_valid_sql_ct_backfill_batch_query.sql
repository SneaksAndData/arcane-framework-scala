CREATE OR REPLACE TABLE test.table_a WITH (partitioning=ARRAY[], format='PARQUET', sorted_by=ARRAY[], parquet_bloom_filter_columns=ARRAY[]) AS
SELECT * FROM test.staged_a AS t_s WHERE t_s.SYS_CHANGE_OPERATION != 'D'
