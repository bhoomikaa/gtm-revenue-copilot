# Revenue Intelligence System (Snowflake-Native)

## Overview

The Revenue Intelligence System is a Snowflake-native analytics application that models go-to-market (GTM) performance end-to-end — from raw ingestion to executive-ready AI insights.

This project demonstrates modern analytics engineering best practices:

- Layered warehouse modeling (RAW → MARTS)
- Governed, reusable metric definitions
- Cohort-based retention logic with boundary safeguards
- Semantic abstraction for consistent dimension and metric reuse
- Built-in data quality validation
- AI-powered interpretation using Snowflake Cortex

The system illustrates how modern SaaS organizations build trusted, AI-ready analytics foundations that support executive decision-making and operational workflows.

---

## Architecture

### 1. Warehouse Modeling (RAW → MARTS)

#### RAW Schema

Stores ingested operational data:

- Accounts  
- Pipeline  
- Stage history  
- Support tickets  

#### MARTS Schema

Contains curated fact tables:

- FCT_MRR — Monthly Recurring Revenue  
- FCT_PIPELINE  
- Derived retention, movement, and coverage logic  

All metrics are computed from fact tables — not dashboards — ensuring governance, traceability, and reusability.

This mirrors enterprise-grade analytics engineering patterns used in mature SaaS organizations.

---

### 2. Governed Metrics

#### Core GTM Metrics

- ARR (MRR × 12)
- Net Revenue Retention (NRR)
- Gross Revenue Retention (GRR)
- Win Rate
- Pipeline Coverage  

- MRR Movement Classification
  - Expansion
  - Contraction
  - Churn
  - New

#### Governance Safeguards

- Cohort-based retention logic  
- Exclusion of incomplete boundary months  
- Prevention of false churn interpretation  
- Strict data-quality gating before NRR/GRR interpretation  
- Clear separation between metric logic and presentation  

All metric logic is centralized and reusable.

---

### 3. Semantic Layer & Governance

A reusable semantic abstraction layer enables:

- Standardized dimensions:
  - Segment  
  - Region  
  - Industry  
  - Rep Team  
- Define-once, reuse-everywhere metric definitions  
- Cross-functional consumption (Sales, Finance, Product)  

Lineage and dependencies were validated in Snowsight to ensure traceability and trust.

---

### 4. Data Quality & Observability

The system includes a lightweight quality layer to ensure interpretability before AI reasoning or executive reporting.

Quality controls include:

- Date bounds validation  
- Row count validation  
- Table integrity checks  
- Boundary-month safeguards for retention  

This prevents misleading interpretations and ensures AI outputs remain grounded in reliable data.

---

### 5. Cortex AI Integration

Snowflake Cortex (AI_COMPLETE) is integrated in a controlled manner.

#### Executive Narrative Generator

- Produces board-level summaries  
- Applies strict retention interpretation rules  
- Prevents hallucinated churn conclusions  
- Grounds narratives in computed metrics  

#### Analyst Q&A (Strict Mode)

- Uses a structured JSON evidence pack  
- Answers only from governed data  
- Includes:
  - Evidence bullets with exact numbers  
  - Interpretability safeguards  
  - Confidence level scoring  

#### Cortex Agent (Multi-Step Reasoning)

Generates:

- Plan  
- Reasoning Chain  
- Answer  
- Evidence  
- Confidence Level  

Demonstrates controlled analytical reasoning over trusted, governed data.

---

## Streamlit Application

The Streamlit app serves as the consumption layer.

Modules include:

- Executive Overview  
- Retention Analysis  
- Pipeline & Stage Dynamics  
- Customer Health  
- Account Explorer  
- Analyst Q&A  
- Data Quality Dashboard  

All metrics are computed in Snowflake and surfaced through a clean UI.

