MERGE INTO test.table_a t_o
USING (SELECT * FROM (
 SELECT * FROM test.staged_a ORDER BY ROW_NUMBER() OVER (PARTITION BY arcane_merge_key ORDER BY arcane_version DESC) FETCH FIRST 1 ROWS WITH TIES
)) t_s
ON t_o.arcane_merge_key = t_s.arcane_merge_key AND (t_o.colA = t_s.colA OR t_s.colA IS NULL)
WHEN MATCHED AND coalesce(t_s.IsDelete, false) = true THEN DELETE
WHEN MATCHED AND coalesce(t_s.IsDelete, false) = false AND t_s.arcane_version > t_o.arcane_version THEN UPDATE SET
 colA = t_s.colA,
colB = t_s.colB,
Id = t_s.Id,
arcane_version = t_s.arcane_version
WHEN NOT MATCHED AND coalesce(t_s.IsDelete, false) = false THEN INSERT (arcane_merge_key,colA,colB,Id,arcane_version) VALUES (t_s.arcane_merge_key,
t_s.colA,
t_s.colB,
t_s.Id,
t_s.arcane_version)
