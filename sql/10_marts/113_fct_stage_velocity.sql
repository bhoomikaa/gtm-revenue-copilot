-- 113_fct_stage_velocity.sql
-- Purpose: Calculate stage duration and funnel metrics

create or replace table GTM_COPILOT.MARTS.FCT_STAGE_VELOCITY as

with stage_durations as (
    select
        opp_id,
        stage,
        stage_start_date,
        stage_end_date,
        datediff(day, stage_start_date, stage_end_date) as stage_duration_days
    from GTM_COPILOT.RAW.OPPORTUNITY_STAGE_HISTORY
    where stage_end_date is not null
),

stage_summary as (
    select
        stage,
        count(distinct opp_id) as deals_reached_stage,
        avg(stage_duration_days) as avg_stage_duration_days
    from stage_durations
    group by stage
)

select *
from stage_summary
order by stage;
