declare @currentVersion bigint = CHANGE_TRACKING_CURRENT_VERSION()

SELECT
{ChangeTrackingColumnsStatement},
@currentVersion AS 'ChangeTrackingVersion'{OptionalMergeKeyColumnExpression}
FROM [{dbName}].[{schema}].[{tableName}] tq
