-- 104_metrics_nrr_monthly.sql
-- Purpose: Monthly Net Revenue Retention (NRR) excluding incomplete months

create or replace table GTM_COPILOT.MARTS.METRICS_NRR_MONTHLY as

with max_valid_month as (
    select max(month) as max_month
    from GTM_COPILOT.MARTS.FCT_MRR_COMPLETE
    where total_mrr > 0
),

base as (
    select
        account_id,
        month,
        total_mrr,
        lag(total_mrr) over (partition by account_id order by month) as previous_mrr
    from GTM_COPILOT.MARTS.FCT_MRR_COMPLETE
),

eligible as (
    select
        account_id,
        month,
        total_mrr,
        previous_mrr
    from base
    where previous_mrr > 0
)

select
    e.month,
    sum(e.previous_mrr) as start_mrr,
    sum(e.total_mrr) as end_mrr,
    round((sum(e.total_mrr) / sum(e.previous_mrr)) * 100, 2) as nrr_pct
from eligible e
join max_valid_month m
    on e.month <= m.max_month
group by e.month
order by e.month;
