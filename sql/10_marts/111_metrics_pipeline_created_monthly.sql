-- 111_metrics_pipeline_created_monthly.sql
-- Purpose: Monthly opportunity creation metrics (pipeline generation)

create or replace table GTM_COPILOT.MARTS.METRICS_PIPELINE_CREATED_MONTHLY as

select
    date_trunc('month', created_date) as created_month,

    -- Number of opportunities created
    count(*) as opportunities_created,

    -- Total value of opportunities created
    sum(amount) as total_created_pipeline_value,

    -- Average deal size
    round(avg(amount), 2) as avg_created_deal_size

from GTM_COPILOT.RAW.OPPORTUNITIES
group by date_trunc('month', created_date)
order by created_month;
