-- 108_fct_pipeline.sql
-- Purpose: Build pipeline fact table for opportunity analytics

create or replace table GTM_COPILOT.MARTS.FCT_PIPELINE as

select
    opp_id,
    account_id,
    product_id,
    rep_id,
    created_date,
    close_date,
    current_stage,
    probability,
    amount,

    -- Weighted pipeline (expected revenue)
    amount * probability as weighted_pipeline,

    is_closed,
    is_won,

    -- Sales cycle (only for closed deals)
    case
        when is_closed = true
        then datediff(day, created_date, close_date)
        else null
    end as sales_cycle_days,

    -- Deal age (only for open deals)
    case
        when is_closed = false
        then datediff(day, created_date, current_date)
        else null
    end as deal_age_days

from GTM_COPILOT.RAW.OPPORTUNITIES;
