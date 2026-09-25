declare @currentVersion bigint = CHANGE_TRACKING_CURRENT_VERSION()

SELECT
{ChangeTrackingColumnsStatement},
@currentVersion AS 'ChangeTrackingVersion'
FROM [{dbName}].[{schema}].[{tableName}] tq
