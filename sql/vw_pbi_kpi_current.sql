view_name,definition
vw_pbi_kpi_current,"CREATE   VIEW dbo.vw_pbi_kpi_current AS
WITH latest_bucket AS (
    SELECT
        ticker,
        MAX(bucket_start_utc) AS latest_bucket_start
    FROM dbo.vw_price_attention_relationship
    GROUP BY ticker
)
SELECT
    r.ticker,
    r.bucket_start_utc        AS as_of_utc,

    -- Price KPIs
    r.bucket_price            AS current_price,
    r.prev_bucket_price,
    r.pct_change_vs_prev      AS price_pct_change,
    r.price_ma_1h,

    -- Attention KPIs
    r.mentions_count,
    r.mentions_avg_1h,
    r.mentions_std_1h,
    r.attention_spike_flag
FROM dbo.vw_price_attention_relationship AS r
JOIN latest_bucket AS lb
    ON  r.ticker           = lb.ticker
    AND r.bucket_start_utc = lb.latest_bucket_start;"
