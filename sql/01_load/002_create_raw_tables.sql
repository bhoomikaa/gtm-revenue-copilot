-- 002_create_raw_tables.sql
-- Purpose: Create RAW tables explicitly before loading data
-- Purpose: Create RAW tables explicitly before loading data

create or replace table GTM_COPILOT.RAW.ACCOUNTS (
    account_id         varchar(50),
    account_name       varchar(255),
    segment            varchar(50),
    region             varchar(50),
    industry           varchar(100),
    created_date       date,
    employee_count     integer,
    owner_rep_id       varchar(50),
    website            varchar(255)
);

copy into GTM_COPILOT.RAW.ACCOUNTS
from @GTM_COPILOT.RAW.RAW_STAGE/accounts.csv
file_format = (format_name = GTM_COPILOT.RAW.CSV_FORMAT)
on_error = 'abort_statement';

create or replace table GTM_COPILOT.RAW.SALES_REPS (
    rep_id      varchar(50),
    rep_name    varchar(255),
    team        varchar(100),
    region      varchar(50),
    email       varchar(255),
    hire_date   date
);
copy into GTM_COPILOT.RAW.SALES_REPS
from @GTM_COPILOT.RAW.RAW_STAGE/sales_reps.csv
file_format = (format_name = GTM_COPILOT.RAW.CSV_FORMAT)
on_error = 'abort_statement';

select count(*) from GTM_COPILOT.RAW.SALES_REPS;

-- PRODUCTS
create or replace table GTM_COPILOT.RAW.PRODUCTS (
    product_id        varchar(50),
    product_name      varchar(255),
    product_family    varchar(100),
    list_price_mrr    numeric(10,2),
    is_addon          boolean
);

-- CONTACTS
create or replace table GTM_COPILOT.RAW.CONTACTS (
    contact_id     varchar(50),
    account_id     varchar(50),
    first_name     varchar(100),
    last_name      varchar(100),
    email          varchar(255),
    phone          varchar(50),
    role           varchar(150),
    is_primary     boolean,
    created_date   date
);

-- DIM_DATE
create or replace table GTM_COPILOT.RAW.DIM_DATE (
    date           date,
    year           integer,
    quarter        integer,
    month          integer,
    month_start    date,
    week           integer,
    day_of_week    integer,
    is_weekend     boolean
);

copy into GTM_COPILOT.RAW.PRODUCTS
from @GTM_COPILOT.RAW.RAW_STAGE/products.csv
file_format = (format_name = GTM_COPILOT.RAW.CSV_FORMAT)
on_error = 'abort_statement';

select count(*) from GTM_COPILOT.RAW.PRODUCTS;

copy into GTM_COPILOT.RAW.CONTACTS
from @GTM_COPILOT.RAW.RAW_STAGE/contacts.csv
file_format = (format_name = GTM_COPILOT.RAW.CSV_FORMAT)
on_error = 'abort_statement';

select count(*) from GTM_COPILOT.RAW.CONTACTS;

copy into GTM_COPILOT.RAW.DIM_DATE
from @GTM_COPILOT.RAW.RAW_STAGE/dim_date.csv
file_format = (format_name = GTM_COPILOT.RAW.CSV_FORMAT)
on_error = 'abort_statement';

select count(*) from GTM_COPILOT.RAW.DIM_DATE;

-- OPPORTUNITIES
create or replace table GTM_COPILOT.RAW.OPPORTUNITIES (
    opp_id             varchar(50),
    account_id         varchar(50),
    product_id         varchar(50),
    rep_id             varchar(50),
    created_date       date,
    close_date         date,
    current_stage      varchar(50),
    probability        numeric(5,2),
    forecast_category  varchar(50),
    amount             numeric(18,2),
    is_closed          boolean,
    is_won             boolean,
    loss_reason        varchar(255),
    competitor         varchar(255)
);

copy into GTM_COPILOT.RAW.OPPORTUNITIES
from @GTM_COPILOT.RAW.RAW_STAGE/opportunities.csv
file_format = (format_name = GTM_COPILOT.RAW.CSV_FORMAT)
on_error = 'abort_statement';

select count(*) from GTM_COPILOT.RAW.OPPORTUNITIES;

-- OPPORTUNITY_STAGE_HISTORY
create or replace table GTM_COPILOT.RAW.OPPORTUNITY_STAGE_HISTORY (
    opp_id             varchar(50),
    account_id         varchar(50),
    stage              varchar(50),
    stage_start_date   date,
    stage_end_date     date
);
-- ACTIVITIES
create or replace table GTM_COPILOT.RAW.ACTIVITIES (
    activity_id    varchar(50),
    opp_id         varchar(50),
    account_id     varchar(50),
    rep_id         varchar(50),
    activity_type  varchar(100),
    activity_date  date,
    notes          varchar(1000)
);
create or replace file format GTM_COPILOT.RAW.CSV_FORMAT
    type = 'CSV'
    field_delimiter = ','
    skip_header = 1
    field_optionally_enclosed_by = '"'
    null_if = ('NULL', 'null', '', 'NaT')
    empty_field_as_null = true;

copy into GTM_COPILOT.RAW.OPPORTUNITY_STAGE_HISTORY
from @GTM_COPILOT.RAW.RAW_STAGE/opportunity_stage_history.csv
file_format = (format_name = GTM_COPILOT.RAW.CSV_FORMAT)
on_error = 'abort_statement';

copy into GTM_COPILOT.RAW.ACTIVITIES
from @GTM_COPILOT.RAW.RAW_STAGE/activities.csv
file_format = (format_name = GTM_COPILOT.RAW.CSV_FORMAT)
on_error = 'abort_statement';

select count(*) from GTM_COPILOT.RAW.OPPORTUNITY_STAGE_HISTORY;
select count(*) from GTM_COPILOT.RAW.ACTIVITIES;

-- =============================
-- REVENUE LAYER TABLES
-- =============================

-- SUBSCRIPTIONS
create or replace table GTM_COPILOT.RAW.SUBSCRIPTIONS (
    subscription_id        varchar(50),
    account_id             varchar(50),
    product_id             varchar(50),
    start_date             date,
    end_date               date,
    contract_term_months   integer,
    billing_frequency      varchar(50),
    currency               varchar(10),
    status                 varchar(50),
    current_mrr            numeric(18,2),
    seat_factor            numeric(10,2)
);

-- SUBSCRIPTION_MONTHLY_MRR
create or replace table GTM_COPILOT.RAW.SUBSCRIPTION_MONTHLY_MRR (
    subscription_id   varchar(50),
    account_id        varchar(50),
    product_id        varchar(50),
    month             date,
    mrr               numeric(18,2),
    event_type        varchar(50)
);

-- INVOICES
create or replace table GTM_COPILOT.RAW.INVOICES (
    invoice_id       varchar(50),
    subscription_id  varchar(50),
    account_id       varchar(50),
    invoice_date     date,
    due_date         date,
    amount           numeric(18,2),
    currency         varchar(10),
    status           varchar(50),
    paid_date        date
);
-- LOAD SUBSCRIPTIONS
copy into GTM_COPILOT.RAW.SUBSCRIPTIONS
from @GTM_COPILOT.RAW.RAW_STAGE/subscriptions.csv
file_format = (format_name = GTM_COPILOT.RAW.CSV_FORMAT)
on_error = 'abort_statement';

-- LOAD SUBSCRIPTION_MONTHLY_MRR
copy into GTM_COPILOT.RAW.SUBSCRIPTION_MONTHLY_MRR
from @GTM_COPILOT.RAW.RAW_STAGE/subscription_monthly_mrr.csv
file_format = (format_name = GTM_COPILOT.RAW.CSV_FORMAT)
on_error = 'abort_statement';

-- LOAD INVOICES
copy into GTM_COPILOT.RAW.INVOICES
from @GTM_COPILOT.RAW.RAW_STAGE/invoices.csv
file_format = (format_name = GTM_COPILOT.RAW.CSV_FORMAT)
on_error = 'abort_statement';

select count(*) from GTM_COPILOT.RAW.SUBSCRIPTIONS;
select count(*) from GTM_COPILOT.RAW.SUBSCRIPTION_MONTHLY_MRR;
select count(*) from GTM_COPILOT.RAW.INVOICES;
