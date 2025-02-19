MERGE INTO test.table_a t_o
USING (SELECT * FROM test.staged_a) t_s
ON t_o.ARCANE_MERGE_KEY = t_s.ARCANE_MERGE_KEY AND (t_o.colA = t_s.colA OR t_s.colA IS NULL)
WHEN MATCHED AND t_s.SYS_CHANGE_OPERATION = 'D' THEN DELETE
WHEN MATCHED AND t_s.SYS_CHANGE_OPERATION != 'D' AND t_s.SYS_CHANGE_VERSION > t_o.SYS_CHANGE_VERSION THEN UPDATE SET
 colA = t_s.colA,
colB = t_s.colB
WHEN NOT MATCHED AND t_s.SYS_CHANGE_OPERATION != 'D' THEN INSERT (ARCANE_MERGE_KEY,colA,colB) VALUES (t_s.ARCANE_MERGE_KEY,
t_s.colA,
t_s.colB)
