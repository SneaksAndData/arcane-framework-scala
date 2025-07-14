MERGE INTO test.table_a t_o
USING (SELECT * FROM (
 SELECT * FROM test.staged_a ORDER BY ROW_NUMBER() OVER (PARTITION BY ARCANE_MERGE_KEY ORDER BY createdon DESC) FETCH FIRST 1 ROWS WITH TIES
)) t_s
ON t_o.ARCANE_MERGE_KEY = t_s.ARCANE_MERGE_KEY AND (t_o.colA = t_s.colA OR t_s.colA IS NULL) AND (t_o.colB = t_s.colB OR t_s.colB IS NULL)
WHEN MATCHED AND t_s.createdon > t_o.createdon THEN UPDATE SET
 colA = t_s.colA,
colB = t_s.colB,
Id = t_s.Id,
createdon = t_s.createdon
WHEN NOT MATCHED  THEN INSERT (ARCANE_MERGE_KEY,colA,colB,Id,createdon) VALUES (t_s.ARCANE_MERGE_KEY,
t_s.colA,
t_s.colB,
t_s.Id,
t_s.createdon)
