-- 001_create_foundation.sql
-- Purpose: Create the GTM Revenue Copilot foundation

-- Step 1: Create the main database
create database if not exists GTM_COPILOT;

-- Step 2: Use the database
use database GTM_COPILOT;

-- Step 3: Create schemas (logical folders)

create schema if not exists RAW;        -- Raw ingested data (bronze)
create schema if not exists MARTS;      -- Cleaned + business logic tables (silver/gold)
create schema if not exists SEMANTIC;   -- Semantic views (governed metrics)
create schema if not exists CORTEX;     -- Search services + AI enrichments
create schema if not exists AGENTS;     -- Agent outputs (recommendations/incidents)
create schema if not exists UTIL;       -- Helper tables (health logs, monitoring)
