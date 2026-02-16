-- 114_metrics_stage_conversion.sql
-- Purpose: Stage-to-stage conversion rates

create or replace table GTM_COPILOT.MARTS.METRICS_STAGE_CONVERSION as

with stage_order as (
    select
        opp_id,
        stage,
        stage_start_date,
        row_number() over (
            partition by opp_id
            order by stage_start_date
        ) as stage_rank
    from GTM_COPILOT.RAW.OPPORTUNITY_STAGE_HISTORY
),

stage_pairs as (
    select
        s1.stage as from_stage,
        s2.stage as to_stage,
        count(distinct s1.opp_id) as deals_progressed
    from stage_order s1
    join stage_order s2
        on s1.opp_id = s2.opp_id
        and s2.stage_rank = s1.stage_rank + 1
    group by s1.stage, s2.stage
),

stage_totals as (
    select
        stage as from_stage,
        count(distinct opp_id) as deals_in_stage
    from GTM_COPILOT.RAW.OPPORTUNITY_STAGE_HISTORY
    group by stage
)

select
    p.from_stage,
    p.to_stage,
    p.deals_progressed,
    t.deals_in_stage,
    round((p.deals_progressed / t.deals_in_stage) * 100, 2) as conversion_rate_pct
from stage_pairs p
join stage_totals t
    on p.from_stage = t.from_stage
order by p.from_stage;
