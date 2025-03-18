MERGE INTO test.table_a t_o
USING (SELECT * FROM (
 SELECT * FROM test.staged_a WHERE ARCANE_BATCH_ID = 'batch_id' ORDER BY ROW_NUMBER() OVER (PARTITION BY ARCANE_MERGE_KEY ORDER BY versionnumber DESC) FETCH FIRST 1 ROWS WITH TIES
)) t_s
ON t_o.ARCANE_MERGE_KEY = t_s.ARCANE_MERGE_KEY AND t_o.colA = t_s.colA AND t_o.colB = t_s.colB
WHEN MATCHED AND coalesce(t_s.IsDelete, false) = true THEN DELETE
WHEN MATCHED AND coalesce(t_s.IsDelete, false) = false AND t_s.versionnumber > t_o.versionnumber THEN UPDATE SET
 colA = t_s.colA,
colB = t_s.colB,
Id = t_s.Id,
versionnumber = t_s.versionnumber
WHEN NOT MATCHED AND coalesce(t_s.IsDelete, false) = false THEN INSERT (ARCANE_MERGE_KEY,colA,colB,Id,versionnumber) VALUES (t_s.ARCANE_MERGE_KEY,
t_s.colA,
t_s.colB,
t_s.Id,
t_s.versionnumber)
