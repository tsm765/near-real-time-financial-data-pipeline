view_name,definition
vw_price_attention_relationship,"CREATE   VIEW dbo.vw_price_attention_relationship AS
SELECT
    -- Prefer price ticker, fallback to mentions ticker
    COALESCE(p.ticker, m.ticker)              AS ticker,
    COALESCE(p.bucket_start_utc, m.bucket_start_utc) AS bucket_start_utc,

    -- Price side
    p.bucket_price,
    p.prev_bucket_price,
    p.pct_change_vs_prev,
    p.price_ma_1h,

    -- Attention side
    m.mentions_count,
    m.mentions_avg_1h,
    m.mentions_std_1h,
    m.is_spike AS attention_spike_flag
FROM dbo.vw_price_timeseries  AS p
FULL OUTER JOIN dbo.vw_mentions_spikes AS m
    ON  p.ticker          = m.ticker
    AND p.bucket_start_utc = m.bucket_start_utc;"
