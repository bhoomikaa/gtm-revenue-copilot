-- 102_fct_mrr_enriched.sql
-- Purpose: Enrich monthly MRR with previous month comparison and movement classification

create or replace table GTM_COPILOT.MARTS.FCT_MRR_ENRICHED as

with base as (
    select
        account_id,
        month,
        total_mrr,
        lag(total_mrr) over (
            partition by account_id
            order by month
        ) as previous_mrr
    from GTM_COPILOT.MARTS.FCT_MRR
),

classified as (
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
    from base
)

select *
from classified;
