-- 109_metrics_pipeline_summary.sql
-- Purpose: Executive pipeline summary metrics

create or replace table GTM_COPILOT.MARTS.METRICS_PIPELINE_SUMMARY as

select

    -- Total open pipeline
    sum(case when is_closed = false then amount else 0 end) as total_open_pipeline,

    -- Weighted open pipeline
    sum(case when is_closed = false then weighted_pipeline else 0 end) as total_weighted_pipeline,

    -- Win rate (closed deals only)
    case 
        when sum(case when is_closed = true then 1 else 0 end) = 0 then null
        else round(
            (
                sum(case when is_won = true then 1 else 0 end)
                /
                sum(case when is_closed = true then 1 else 0 end)
            ) * 100,
            2
        )
    end as win_rate_pct,

    -- Average sales cycle (closed deals only)
    round(
        avg(case when is_closed = true then sales_cycle_days end),
        2
    ) as avg_sales_cycle_days

from GTM_COPILOT.MARTS.FCT_PIPELINE;
