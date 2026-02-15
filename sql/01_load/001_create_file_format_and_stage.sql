-- 001_create_file_format_and_stage.sql
-- Purpose: Define CSV format + create internal stage for raw ingestion

create or replace file format GTM_COPILOT.RAW.CSV_FORMAT
    type = 'CSV'
    field_delimiter = ','
    skip_header = 1
    field_optionally_enclosed_by = '"'
    null_if = ('NULL', 'null', '')
    empty_field_as_null = true;

create or replace stage GTM_COPILOT.RAW.RAW_STAGE
    file_format = GTM_COPILOT.RAW.CSV_FORMAT;
