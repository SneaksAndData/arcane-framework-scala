MERGE INTO test.table_a t_o
USING (SELECT * FROM (
SELECT * FROM test.staged_a ORDER BY ROW_NUMBER() OVER (PARTITION BY arcane_merge_key ORDER BY SYS_CHANGE_VERSION DESC) FETCH FIRST 1 ROWS WITH TIES
)) t_s
ON t_o.arcane_merge_key = t_s.arcane_merge_key
WHEN MATCHED AND t_s.SYS_CHANGE_OPERATION = 'D' THEN DELETE
WHEN MATCHED AND t_s.SYS_CHANGE_OPERATION != 'D' AND t_s.SYS_CHANGE_VERSION > t_o.SYS_CHANGE_VERSION THEN UPDATE SET
 colA = t_s.colA,
colB = t_s.colB
WHEN NOT MATCHED AND t_s.SYS_CHANGE_OPERATION != 'D' THEN INSERT (arcane_merge_key,colA,colB) VALUES (t_s.arcane_merge_key,
t_s.colA,
t_s.colB)
