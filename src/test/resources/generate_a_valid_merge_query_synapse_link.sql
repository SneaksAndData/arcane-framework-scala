MERGE INTO test.table_a t_o
USING (SELECT * FROM (
 SELECT * FROM test.staged_a ORDER BY ROW_NUMBER() OVER (PARTITION BY Id ORDER BY versionnumber DESC) FETCH FIRST 1 ROWS WITH TIES
)) t_s
ON t_o.ARCANE_MERGE_KEY = t_s.ARCANE_MERGE_KEY
WHEN MATCHED AND t_s.IsDelete = true THEN DELETE
WHEN MATCHED AND t_s.IsDelete = false AND t_s.versionnumber > t_o.versionnumber THEN UPDATE SET
 colA = t_s.colA,
colB = t_s.colB,
Id = t_s.Id,
versionnumber = t_s.versionnumber
WHEN NOT MATCHED AND t_s.IsDelete = false THEN INSERT (ARCANE_MERGE_KEY,colA,colB,Id,versionnumber) VALUES (t_s.ARCANE_MERGE_KEY,
t_s.colA,
t_s.colB,
t_s.Id,
t_s.versionnumber)
