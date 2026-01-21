view_name,definition
vw_mentions_5min,"CREATE   VIEW dbo.vw_mentions_5min AS
WITH mention_events AS (
    SELECT *
    FROM dbo.vw_unified_events_clean
    WHERE event_type = 'mention'
),
bucketed AS (
    SELECT
        ticker,
        -- 5-minute bucket start time
        DATEADD(
            MINUTE,
            DATEDIFF(MINUTE, 0, event_time_utc) / 5 * 5,
            0
        ) AS bucket_start_utc
    FROM mention_events
)
SELECT
    ticker,
    bucket_start_utc,
    COUNT(*) AS mentions_count
FROM bucketed
GROUP BY
    ticker,
    bucket_start_utc;"
