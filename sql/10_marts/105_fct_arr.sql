-- 105_fct_arr.sql
-- Purpose: Create ARR fact table (annualized recurring revenue)

create or replace table GTM_COPILOT.MARTS.FCT_ARR as

select
    account_id,
    month,
    total_mrr,
    total_mrr * 12 as arr
from GTM_COPILOT.MARTS.FCT_MRR_COMPLETE;
