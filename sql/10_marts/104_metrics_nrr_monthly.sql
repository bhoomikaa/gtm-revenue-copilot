-- 104_metrics_nrr_monthly.sql
-- Purpose: Monthly Net Revenue Retention (NRR) across existing customer base

create or replace table GTM_COPILOT.MARTS.METRICS_NRR_MONTHLY as

with base as (
    select
        account_id,
        month,
        total_mrr,
        lag(total_mrr) over (partition by account_id order by month) as previous_mrr
    from GTM_COPILOT.MARTS.FCT_MRR_COMPLETE
),

eligible as (
    -- "Existing customers" for a month are those who had revenue in the previous month
    select
        account_id,
        month,
        total_mrr,
        previous_mrr
    from base
    where previous_mrr > 0
)

select
    month,
    sum(previous_mrr) as start_mrr,
    sum(total_mrr) as end_mrr,
    case
        when sum(previous_mrr) = 0 then null
        else round( (sum(total_mrr) / sum(previous_mrr)) * 100, 2 )
    end as nrr_pct
from eligible
group by month
order by month;
