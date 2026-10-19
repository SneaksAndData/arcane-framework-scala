select
    bc.COLUMN_NAME,
    case when kcu.CONSTRAINT_NAME is not null then 1 else 0 end as IsPrimaryKey
from
    [{dbName}].INFORMATION_SCHEMA.COLUMNS as bc
    left join [{dbName}].INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc on bc.TABLE_SCHEMA = tc.TABLE_SCHEMA and bc.TABLE_NAME = tc.TABLE_NAME
    left join [{dbName}].INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu on tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME and bc.COLUMN_NAME = kcu.COLUMN_NAME
where
    bc.TABLE_NAME = N'{table}'
    and bc.TABLE_SCHEMA = N'{schema}'
order by bc.ORDINAL_POSITION
