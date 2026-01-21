view_name,definition
vw_mentions_spikes,"CREATE   VIEW dbo.vw_mentions_spikes AS
SELECT
    ticker,
    bucket_start_utc,
    mentions_count,

    -- 1-hour rolling window = 12 x 5-min buckets
    AVG(CAST(mentions_count AS FLOAT)) OVER (
        PARTITION BY ticker
        ORDER BY bucket_start_utc
        ROWS BETWEEN 12 PRECEDING AND CURRENT ROW
    ) AS mentions_avg_1h,

    STDEV(CAST(mentions_count AS FLOAT)) OVER (
        PARTITION BY ticker
        ORDER BY bucket_start_utc
        ROWS BETWEEN 12 PRECEDING AND CURRENT ROW
    ) AS mentions_std_1h,

    CASE
        WHEN AVG(CAST(mentions_count AS FLOAT)) OVER (
                 PARTITION BY ticker
                 ORDER BY bucket_start_utc
                 ROWS BETWEEN 12 PRECEDING AND CURRENT ROW
             ) IS NULL
          OR STDEV(CAST(mentions_count AS FLOAT)) OVER (
                 PARTITION BY ticker
                 ORDER BY bucket_start_utc
                 ROWS BETWEEN 12 PRECEDING AND CURRENT ROW
             ) IS NULL
        THEN 0
        WHEN CAST(mentions_count AS FLOAT) >
             AVG(CAST(mentions_count AS FLOAT)) OVER (
                 PARTITION BY ticker
                 ORDER BY bucket_start_utc
                 ROWS BETWEEN 12 PRECEDING AND CURRENT ROW
             )
             + 2 * STDEV(CAST(mentions_count AS FLOAT)) OVER (
                 PARTITION BY ticker
                 ORDER BY bucket_start_utc
                 ROWS BETWEEN 12 PRECEDING AND CURRENT ROW
             )
        THEN 1
        ELSE 0
    END AS is_spike
FROM dbo.vw_mentions_5min;"
