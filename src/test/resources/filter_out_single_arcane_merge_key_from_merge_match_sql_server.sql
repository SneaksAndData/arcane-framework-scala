MERGE INTO test.table_a t_o
USING (
SELECT
t_s.*
FROM test.staged_a AS t_s inner join (SELECT ARCANE_MERGE_KEY, MAX(SYS_CHANGE_VERSION) AS LATEST_VERSION FROM test.staged_a GROUP BY ARCANE_MERGE_KEY) as v
on t_s.ARCANE_MERGE_KEY = v.ARCANE_MERGE_KEY AND t_s.SYS_CHANGE_VERSION = v.LATEST_VERSION
) t_s
ON t_o.ARCANE_MERGE_KEY = t_s.ARCANE_MERGE_KEY
WHEN MATCHED AND t_s.SYS_CHANGE_OPERATION = 'D' THEN DELETE
WHEN MATCHED AND t_s.SYS_CHANGE_OPERATION != 'D' AND t_s.SYS_CHANGE_VERSION > t_o.SYS_CHANGE_VERSION THEN UPDATE SET
 colA = t_s.colA,
colB = t_s.colB
WHEN NOT MATCHED AND t_s.SYS_CHANGE_OPERATION != 'D' THEN INSERT (ARCANE_MERGE_KEY,colA,colB) VALUES (t_s.ARCANE_MERGE_KEY,
t_s.colA,
t_s.colB)
