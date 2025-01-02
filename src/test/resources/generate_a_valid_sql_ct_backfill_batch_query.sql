INSERT OVERWRITE test.table_a
SELECT * FROM test.staged_a AS t_s WHERE t_s.SYS_CHANGE_OPERATION != 'D'
