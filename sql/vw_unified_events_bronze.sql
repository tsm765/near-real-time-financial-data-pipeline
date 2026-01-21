view_name,definition
vw_unified_events_bronze,"CREATE   VIEW dbo.vw_unified_events_bronze AS
SELECT
    event_type,
    ticker,
    price,
    mentions,
    [timestamp],
    source,
    ingest_time,
    topic
FROM OPENROWSET(
    BULK 'abfss://finstream@my1storage2account3.dfs.core.windows.net/curated/unified_events/year=*/month=*/day=*/unified_*.parquet',
    FORMAT = 'PARQUET'
) AS rows;"
