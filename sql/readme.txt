Revenue Intelligence System (Snowflake-Native)
Overview

This project is a Snowflake-native Revenue Intelligence application that models go-to-market performance end-to-end — from raw ingestion to executive-ready AI insights.

The system demonstrates analytics engineering best practices: layered data modeling (RAW → MARTS), governed metric definitions, cohort-based retention logic with boundary safeguards, semantic abstraction for reusable dimensions/measures, data quality checks, and AI-powered interpretation using Snowflake Cortex.

The goal is to show how modern SaaS organizations can build trusted, AI-ready analytics foundations that support both executive decision-making and operational teams.

Architecture
1. Warehouse Modeling (RAW → MARTS)

RAW schema stores ingested operational data (accounts, pipeline, stage history, tickets).

MARTS schema contains curated fact tables:

FCT_MRR (monthly recurring revenue)

FCT_PIPELINE

Derived retention, movement, and coverage logic

Metrics are computed from fact tables, not dashboards, ensuring governance and reusability.

This mirrors enterprise-grade analytics engineering patterns used in SaaS organizations.

2. Governed Metrics

Core GTM metrics implemented:

ARR (derived from MRR × 12)

NRR / GRR (cohort-based, excluding incomplete boundary months)

Win Rate

Pipeline Coverage (Open Pipeline ÷ Avg 3-Month Closed Revenue)

MRR Movement Classification (Expansion, Contraction, Churn, New)

Special attention was given to:

Preventing false churn interpretation when retention data is incomplete

Strict data-quality gating before interpreting NRR/GRR

Clear separation between metric logic and presentation

3. Semantic Layer & Governance

A reusable semantic abstraction was implemented to:

Standardize dimensions (Segment, Region, Industry, Rep Team)

Define metrics once and reuse across visualizations

Support cross-functional consumption (Sales, Finance, Product)

Lineage and dependencies were validated in Snowsight to ensure traceability and trust.

4. Data Quality & Observability

The app includes a lightweight quality layer showing:

Date bounds

Row counts

Table validation

Boundary-month safeguards for retention

This ensures interpretability before AI reasoning or executive reporting.

5. Cortex AI Integration

Snowflake Cortex (AI_COMPLETE) is used in two ways:

Executive Narrative Generator

Produces board-level summaries

Strict retention interpretation rules

Prevents hallucinated churn conclusions

Grounded in actual computed metrics

Analyst Q&A (Strict Mode)

Uses a structured JSON “evidence pack”

Answers only from governed data

Includes:

Evidence bullets with exact numbers

Clear interpretability safeguards

Confidence Level scoring

Cortex Agent (Multi-Step Reasoning)

Generates:

Plan

Reasoning Chain

Answer

Evidence

Confidence Level

Demonstrates controlled multi-step analytical reasoning over trusted data.

Streamlit Application

The Streamlit app serves as the consumption layer:

Executive Overview

Retention Analysis

Pipeline & Stage Dynamics

Customer Health

Account Explorer

Analyst Q&A

Data Quality Dashboard

All metrics are computed in Snowflake and surfaced through a clean UI.

Purpose

This project demonstrates:

Analytics engineering discipline

Metric governance

Data modeling best practices

AI-ready data foundations

Responsible AI integration (grounded, controlled inference)

Cross-functional SaaS reporting architecture

It reflects how a mature Data, Analytics & AI organization structures semantic models, enforces metric consistency, and integrates AI safely into operational workflows.