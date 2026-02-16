-- 103_fct_mrr_complete.sql
-- Purpose: Create full account × month grid and explicitly model zero MRR

create or replace table GTM_COPILOT.MARTS.FCT_MRR_COMPLETE as

with months as (
    -- Get one row per month from date dimension
    select distinct
        month_start as month
    from GTM_COPILOT.RAW.DIM_DATE
),

accounts as (
    select distinct account_id
    from GTM_COPILOT.RAW.ACCOUNTS
),

account_month_grid as (
    -- Create full account × month combinations
    select
        a.account_id,
        m.month
    from accounts a
    cross join months m
),

mrr_joined as (
    -- Join actual revenue data
    select
        g.account_id,
        g.month,
        coalesce(f.total_mrr, 0) as total_mrr
    from account_month_grid g
    left join GTM_COPILOT.MARTS.FCT_MRR f
        on g.account_id = f.account_id
        and g.month = f.month
),

movement as (
    select
        account_id,
        month,
        total_mrr,
        lag(total_mrr) over (
            partition by account_id
            order by month
        ) as previous_mrr
    from mrr_joined
)

select
    account_id,
    month,
    total_mrr,
    coalesce(previous_mrr, 0) as previous_mrr,
    total_mrr - coalesce(previous_mrr, 0) as mrr_change,
    case
        when coalesce(previous_mrr, 0) = 0 and total_mrr > 0 then 'New'
        when previous_mrr > 0 and total_mrr = 0 then 'Churn'
        when total_mrr > previous_mrr then 'Expansion'
        when total_mrr < previous_mrr and total_mrr > 0 then 'Contraction'
        else 'Flat'
    end as movement_type
from movement;
