All analytics views used by Power BI are created in Azure Synapse Serverless SQL.
For transparency and version control, the view definitions are included in the `sql/` folder.

Included views:
- `vw_unified_events_bronze` – raw unified events from Kafka (bronze layer)
- `vw_unified_events_clean` – cleaned and standardized events (silver layer)
- `vw_price_timeseries` – time-bucketed price data
- `vw_mentions_5mins` – 5-minute aggregated news mentions
- `vw_mentions_spikes` – spike detection logic on mentions
- `vw_price_attention_relationship` – joined price vs attention analytics
- `vw_pbi_kpi_current` – current KPI snapshot used by Power BI
