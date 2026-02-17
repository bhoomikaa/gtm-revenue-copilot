-- 107_metrics_grr_monthly.sql
-- Purpose: Monthly Gross Revenue Retention (GRR)

create or replace table GTM_COPILOT.MARTS.METRICS_GRR_MONTHLY as

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
    -- Only customers that had revenue in the previous month
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

    -- Cap current revenue at previous revenue (ignore expansion)
    sum(
        case
            when e.total_mrr >= e.previous_mrr then e.previous_mrr
            else e.total_mrr
        end
    ) as retained_mrr,

    round(
        (
            sum(
                case
                    when e.total_mrr >= e.previous_mrr then e.previous_mrr
                    else e.total_mrr
                end
            ) / sum(e.previous_mrr)
        ) * 100,
        2
    ) as grr_pct

from eligible e
join max_valid_month m
    on e.month <= m.max_month
group by e.month
order by e.month;
