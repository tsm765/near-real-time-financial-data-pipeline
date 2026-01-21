view_name,definition
vw_unified_events_clean,"CREATE   VIEW dbo.vw_unified_events_clean AS
SELECT
    LOWER(event_type)                    AS event_type,
    UPPER(ticker)                        AS ticker,          -- normalize ticker
    CAST(price AS FLOAT)                 AS price,
    CAST(mentions AS INT)                AS mentions,

       CAST(
      DATEADD(
        MILLISECOND,
        CAST(([timestamp] / 1000000) % 1000 AS INT),
        DATEADD(
          SECOND,
          CAST([timestamp] / 1000000000 AS INT),
          '1970-01-01'
        )
      ) AS DATETIME2(3)
    ) AS event_time_utc,

    LOWER(source)                        AS source,

    CAST(
      DATEADD(
        MILLISECOND,
        CAST((ingest_time / 1000000) % 1000 AS INT),
        DATEADD(
          SECOND,
          CAST(ingest_time / 1000000000 AS INT),
          '1970-01-01'
        )
      ) AS DATETIME2(3)
    ) AS ingest_time_utc,

    LOWER(topic)                         AS topic
FROM dbo.vw_unified_events_bronze
WHERE ticker IS NOT NULL
  AND [timestamp] IS NOT NULL;"
