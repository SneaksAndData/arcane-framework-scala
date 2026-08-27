SELECT
{ChangeTrackingColumnsStatement},
cast({lastId} as bigint) AS 'ChangeTrackingVersion'
FROM [{dbName}].[{schema}].[{tableName}] tq
RIGHT JOIN (SELECT ct.* FROM CHANGETABLE (CHANGES [{dbName}].[{schema}].[{tableName}], {lastId}) ct ) ct ON {ChangeTrackingMatchStatement}
