-- 110_metrics_pipeline_monthly.sql
-- Purpose: Monthly closed deal performance metrics

create or replace table GTM_COPILOT.MARTS.METRICS_PIPELINE_MONTHLY as

select
    date_trunc('month', close_date) as close_month,

    -- Total revenue from closed deals
    sum(amount) as total_closed_revenue,

    -- Revenue from won deals only
    sum(case when is_won = true then amount else 0 end) as total_won_revenue,

    -- Win rate (closed deals only)
    case
        when count(*) = 0 then null
        else round(
            (
                sum(case when is_won = true then 1 else 0 end)
                /
                count(*)
            ) * 100,
            2
        )
    end as win_rate_pct,

    -- Average sales cycle for closed deals
    round(avg(sales_cycle_days), 2) as avg_sales_cycle_days

from GTM_COPILOT.MARTS.FCT_PIPELINE
where is_closed = true
group by date_trunc('month', close_date)
order by close_month;
