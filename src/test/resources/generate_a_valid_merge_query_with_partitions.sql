MERGE INTO test.table_a t_o
USING (SELECT * FROM test.staged_a) t_s
ON t_o.arcane_merge_key = t_s.arcane_merge_key AND (t_o.colA = t_s.colA OR t_s.colA IS NULL)
WHEN MATCHED AND t_s.SYS_CHANGE_OPERATION = 'D' THEN DELETE
WHEN MATCHED AND t_s.SYS_CHANGE_OPERATION != 'D' AND t_s.arcane_version > t_o.arcane_version THEN UPDATE SET
 colA = t_s.colA,
colB = t_s.colB
WHEN NOT MATCHED AND t_s.SYS_CHANGE_OPERATION != 'D' THEN INSERT (arcane_merge_key,colA,colB) VALUES (t_s.arcane_merge_key,
t_s.colA,
t_s.colB)
