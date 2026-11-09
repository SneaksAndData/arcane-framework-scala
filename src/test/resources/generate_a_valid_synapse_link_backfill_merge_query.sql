MERGE INTO test.table_a t_o
USING (SELECT * FROM test.staged_a) t_s
ON t_o.arcane_merge_key = t_s.arcane_merge_key AND (t_o.colA = t_s.colA OR t_s.colA IS NULL) AND (t_o.colB = t_s.colB OR t_s.colB IS NULL)
WHEN MATCHED AND coalesce(t_s.IsDelete, false) = true THEN DELETE
WHEN MATCHED AND coalesce(t_s.IsDelete, false) = false AND t_s.versionnumber > t_o.versionnumber THEN UPDATE SET
 colA = t_s.colA,
colB = t_s.colB,
Id = t_s.Id,
versionnumber = t_s.versionnumber
WHEN NOT MATCHED AND coalesce(t_s.IsDelete, false) = false THEN INSERT (arcane_merge_key,colA,colB,Id,versionnumber) VALUES (t_s.arcane_merge_key,
t_s.colA,
t_s.colB,
t_s.Id,
t_s.versionnumber)
