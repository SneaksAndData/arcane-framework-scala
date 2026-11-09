MERGE INTO test.table_a t_o
USING (SELECT * FROM (
 SELECT * FROM test.staged_a ORDER BY ROW_NUMBER() OVER (PARTITION BY arcane_merge_key ORDER BY createdon DESC) FETCH FIRST 1 ROWS WITH TIES
)) t_s
ON t_o.arcane_merge_key = t_s.arcane_merge_key
WHEN MATCHED AND t_s.createdon > t_o.createdon THEN UPDATE SET
 colA = t_s.colA,
colB = t_s.colB,
Id = t_s.Id,
createdon = t_s.createdon
WHEN NOT MATCHED  THEN INSERT (arcane_merge_key,colA,colB,Id,createdon) VALUES (t_s.arcane_merge_key,
t_s.colA,
t_s.colB,
t_s.Id,
t_s.createdon)
