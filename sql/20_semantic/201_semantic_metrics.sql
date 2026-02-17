-- 201_semantic_metrics.sql
-- Purpose: Unified governed GTM metrics view

create or replace view GTM_COPILOT.SEMANTIC.V_GTM_METRICS as

with arr as (
    select
        month,
        total_arr
    from GTM_COPILOT.MARTS.METRICS_ARR_MONTHLY
),

nrr as (
    select
        month,
        nrr_pct
    from GTM_COPILOT.MARTS.METRICS_NRR_MONTHLY
),

grr as (
    select
        month,
        grr_pct
    from GTM_COPILOT.MARTS.METRICS_GRR_MONTHLY
),

pipeline as (
    select
        close_month as month,
        total_closed_revenue,
        win_rate_pct
    from GTM_COPILOT.MARTS.METRICS_PIPELINE_MONTHLY
),

coverage as (
    select
        pipeline_coverage_ratio
    from GTM_COPILOT.MARTS.METRICS_PIPELINE_COVERAGE
)

select
    a.month,
    a.total_arr,
    n.nrr_pct,
    g.grr_pct,
    p.total_closed_revenue,
    p.win_rate_pct,
    c.pipeline_coverage_ratio

from arr a
left join nrr n on a.month = n.month
left join grr g on a.month = g.month
left join pipeline p on a.month = p.month
cross join coverage c
order by a.month;
