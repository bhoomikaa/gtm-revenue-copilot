-- 112_metrics_pipeline_coverage.sql

create or replace table GTM_COPILOT.MARTS.METRICS_PIPELINE_COVERAGE as

with latest_revenue as (
    select
        max(close_month) as latest_month
    from GTM_COPILOT.MARTS.METRICS_PIPELINE_MONTHLY
),

last_closed as (
    select
        m.close_month,
        m.total_closed_revenue
    from GTM_COPILOT.MARTS.METRICS_PIPELINE_MONTHLY m
    join latest_revenue l
        on m.close_month = l.latest_month
),

open_pipeline as (
    select
        sum(amount) as total_open_pipeline
    from GTM_COPILOT.MARTS.FCT_PIPELINE
    where is_closed = false
)

select
    o.total_open_pipeline,
    l.total_closed_revenue,
    round(o.total_open_pipeline / nullif(l.total_closed_revenue,0), 2) as pipeline_coverage_ratio
from open_pipeline o
cross join last_closed l;
