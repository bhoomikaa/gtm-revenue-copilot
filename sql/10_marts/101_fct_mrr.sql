-- 101_fct_mrr.sql
-- Purpose: Create account-level monthly recurring revenue fact table

create or replace table GTM_COPILOT.MARTS.FCT_MRR as

select
    account_id,
    month,
    sum(mrr) as total_mrr
from GTM_COPILOT.RAW.SUBSCRIPTION_MONTHLY_MRR
group by
    account_id,
    month;
