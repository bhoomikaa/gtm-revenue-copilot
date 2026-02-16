-- 106_metrics_arr_monthly.sql
-- Purpose: Company-level ARR per month (executive metric)

create or replace table GTM_COPILOT.MARTS.METRICS_ARR_MONTHLY as

with max_valid_month as (
    select max(month) as max_month
    from GTM_COPILOT.MARTS.FCT_MRR_COMPLETE
    where total_mrr > 0
)

select
    f.month,
    sum(f.arr) as total_arr
from GTM_COPILOT.MARTS.FCT_ARR f
join max_valid_month m
    on f.month <= m.max_month
group by f.month
order by f.month;
