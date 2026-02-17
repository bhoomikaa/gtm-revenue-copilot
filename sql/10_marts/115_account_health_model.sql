-- 115_account_health_model.sql
-- Purpose: Advanced Account Health Snapshot

create or replace table GTM_COPILOT.MARTS.FCT_ACCOUNT_HEALTH as

with latest_month as (
    select max(month) as max_month
    from GTM_COPILOT.MARTS.FCT_MRR_COMPLETE
    where total_mrr > 0
),

recent_revenue as (
    select
        f.account_id,
        f.month,
        f.total_mrr,
        lag(f.total_mrr,1) over (partition by f.account_id order by f.month) as mrr_1m_ago,
        lag(f.total_mrr,2) over (partition by f.account_id order by f.month) as mrr_2m_ago
    from GTM_COPILOT.MARTS.FCT_MRR_COMPLETE f
),

latest_revenue as (
    select r.*
    from recent_revenue r
    join latest_month l
        on r.month = l.max_month
),

open_pipeline as (
    select
        account_id,
        sum(amount) as open_pipeline_value
    from GTM_COPILOT.MARTS.FCT_PIPELINE
    where is_closed = false
    group by account_id
)

select
    a.account_id,
    coalesce(l.total_mrr,0) as latest_mrr,
    coalesce(l.mrr_1m_ago,0) as mrr_1m_ago,
    coalesce(l.mrr_2m_ago,0) as mrr_2m_ago,
    coalesce(p.open_pipeline_value,0) as open_pipeline_value,

    -- Revenue trend detection
    case
        when l.total_mrr > l.mrr_1m_ago
         and l.mrr_1m_ago > l.mrr_2m_ago then 'Growing'
        when l.total_mrr < l.mrr_1m_ago
         and l.mrr_1m_ago < l.mrr_2m_ago then 'Declining'
        else 'Stable'
    end as mrr_trend_3m,

    -- High value account flag
    case
        when l.total_mrr > 100000 then 1
        else 0
    end as high_value_flag,

    -- Health score logic
    (
        case
            when l.total_mrr = 0 then -3
            when l.total_mrr < l.mrr_1m_ago then -1
            when l.total_mrr > l.mrr_1m_ago then 1
            else 0
        end
        +
        case
            when coalesce(p.open_pipeline_value,0) > 0 then 1
            else 0
        end
    ) as health_score,

    case
        when l.total_mrr = 0 then 'Lost'
        when l.total_mrr < l.mrr_1m_ago and coalesce(p.open_pipeline_value,0)=0 then 'High Risk'
        when l.total_mrr < l.mrr_1m_ago then 'At Risk'
        when l.total_mrr > l.mrr_1m_ago then 'Growing'
        else 'Stable'
    end as health_status

from GTM_COPILOT.RAW.ACCOUNTS a
left join latest_revenue l on a.account_id = l.account_id
left join open_pipeline p on a.account_id = p.account_id;
