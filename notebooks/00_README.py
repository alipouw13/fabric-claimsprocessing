# Databricks notebook source
# MAGIC %md
# MAGIC # Smart Claims on Microsoft Fabric
# MAGIC 
# MAGIC <br>
# MAGIC <img src="https://github.com/databricks-industry-solutions/smart-claims/raw/main/resource/images/InsuranceReferenceArchitecture.png" width="65%" />
# MAGIC 
# MAGIC ## Why this matters for Insurance
# MAGIC Modern P&C insurers face simultaneous pressure to (1) reduce loss & leakage ratios, (2) accelerate claims cycle times for superior customer experience, and (3) combat increasingly sophisticated fraud while innovating with new usage‑based & personalized products. Microsoft Fabric unifies data, analytics, ML and BI in a single SaaS platform—reducing integration friction so actuarial, claims, fraud, underwriting, pricing, and data science teams derive value faster.
# MAGIC 
# MAGIC This Smart Claims Accelerator shows how to operationalize a claims intelligence pipeline using Fabric Lakehouse patterns (Bronze → Silver → Gold) plus real‑time, ML & rules augmentation for decision support and automation.
# MAGIC 
# MAGIC ---
# MAGIC ## Business Challenges (Domain Context)
# MAGIC * Lower operational & loss adjustment expenses while sustaining profitability
# MAGIC * Retain policyholders in a price‑sensitive, high‑churn environment
# MAGIC * Detect & deter fraud (organized & opportunistic) early in the lifecycle
# MAGIC * Shorten time to first decision & settlement (customer NPS & cost impact)
# MAGIC * Prioritize adjuster workload with explainable risk signals
# MAGIC * Leverage telematics & image evidence consistently and at scale
# MAGIC 
# MAGIC ## Target Outcomes
# MAGIC | Objective | Fabric-Enabled Outcome |
# MAGIC |-----------|------------------------|
# MAGIC | Faster cycle time | Automated enrichment & triage reduces manual touches |
# MAGIC | Fraud containment | ML + rules flags anomalies early |
# MAGIC | Cost discipline | Unified lakehouse reduces duplication / ETL sprawl |
# MAGIC | Customer trust | Consistent, explainable AI decisions (audit trail) |
# MAGIC | Innovation velocity | Single SaaS platform (OneLake + Power BI + ML) accelerates iteration |
# MAGIC 
# MAGIC ---
# MAGIC ## Fabric Capabilities Mapped to Smart Claims
# MAGIC | Fabric Capability | Role in Solution |
# MAGIC |-------------------|------------------|
# MAGIC | OneLake / Lakehouse | Unified storage for Bronze/Silver/Gold datasets |
# MAGIC | Data Factory (Pipelines) | Scheduled ingestion of policies, claims, telematics, images metadata |
# MAGIC | Data Engineering (Spark Notebooks) | Incremental curation, feature enrichment, partitioned Delta tables |
# MAGIC | Real-Time Analytics (Eventstreams / KQL DB) | (Extendable) ingestion of streaming telematics for near real‑time scoring |
# MAGIC | Data Science / MLflow in Fabric | Model registration, versioning, inference tracking |
# MAGIC | Azure OpenAI + Fabric Integration | (Optional) Vision / NLP for damage estimation, narrative summarization |
# MAGIC | Power BI Direct Lake | Low‑latency dashboards (Loss Summary, Claims Investigation) without import refresh lag |
# MAGIC | Purview (Governance) | Lineage, data classification, policy enforcement |
# MAGIC | Security (Entra ID, RLS/OLS) | Fine‑grained access to sensitive claim elements |
# MAGIC | Fabric Semantic Model | Business-friendly layer for KPIs & actuarial metrics |
# MAGIC 
# MAGIC ---
# MAGIC ## Solution Overview (Smart Claims Flow)
# MAGIC 1. Ingest raw claim, policy, telematics & image metadata into **Bronze** tables (append-only, minimal transformations)
# MAGIC 2. Refine & standardize into **Silver** (joins, quality scoring, ML severity inference, rule flags)
# MAGIC 3. Publish analytic & risk aggregates to **Gold** for BI / decisioning
# MAGIC 4. Expose curated measures via Direct Lake to Power BI dashboards (Loss ratio, severity distributions, fraud indicators)
# MAGIC 5. (Optional) Feedback loop to operational systems (workflow, alerting, claim routing)
# MAGIC 
# MAGIC The accelerator includes logic for chunked, memory‑safe image severity scoring (simulated ML in this repo) ready to swap with a registered model (MLflow or Azure OpenAI Vision / custom PyTorch).
# MAGIC 
# MAGIC ---
# MAGIC ## Data Model (Simplified Domain)
# MAGIC <img src="https://github.com/databricks-industry-solutions/smart-claims/raw/main/resource/images/domain_model.png" width="60%" />
# MAGIC 
# MAGIC Core Entities (sample subset): Policy, Claim, Accident/Image, Telematics Trip, Driver. Relationships enable multi‑fact augmentation (policy limits, behavioral risk, severity outputs) to form composite risk views.
# MAGIC 
# MAGIC ---
# MAGIC ## Medallion Architecture in Fabric
# MAGIC <img src="https://github.com/databricks-industry-solutions/smart-claims/raw/main/resource/images/medallion_architecture_dlt.png" width="70%" />
# MAGIC (Diagram concept reused – orchestration is implemented with Fabric Pipelines / Notebooks instead of DLT in this adaptation.)
# MAGIC 
# MAGIC * Bronze: bronze_claim, bronze_policy, bronze_accident, bronze_telematics
# MAGIC * Silver: silver_claim, silver_policy, silver_accident (ML enriched), joins (claim_policy_accident, claim_policy_telematics)
# MAGIC * Gold: risk & fraud insight tables, KPI aggregates, rules outcomes
# MAGIC 
# MAGIC ---
# MAGIC ## ML + Rules Augmentation
# MAGIC | Layer | Purpose |
# MAGIC |-------|---------|
# MAGIC | ML Severity (image) | Estimate damage severity → triage & reserve accuracy |
# MAGIC | Telematics Features | Speed patterns, location concordance, impact context |
# MAGIC | Rule Engine | Codified business / compliance checks (coverage period, severity agreement, location mismatch, limit validation) |
# MAGIC | Composite Risk Signals | Prioritize adjuster queue, flag potential fraud, accelerate straight‑through processing |
# MAGIC 
# MAGIC ---
# MAGIC ## Dashboards (Power BI Direct Lake)
# MAGIC * Loss Summary: loss ratio trends, incident frequency, severity mix, geographic heat overlays
# MAGIC * Claims Investigation: per‑claim drill (images, telematics trace, rule flags, severity vs reported)
# MAGIC 
# MAGIC Direct Lake eliminates import refresh latency, querying Gold model columns directly from OneLake Delta representations.
# MAGIC 
# MAGIC ---
# MAGIC ## How to Use in Microsoft Fabric
# MAGIC 1. Create / open a Fabric Workspace (with Lakehouse & Data Engineering enabled)
# MAGIC 2. Use Git integration (Connect to repo) and point to this repository
# MAGIC 3. Create a Lakehouse (e.g., `smart_claims_lakehouse`) and ensure it is the default for notebooks
# MAGIC 4. Upload synthetic data (if not already present) under `Files/resource/data_sources/` or run provided setup script (`setup/initialize.py` if adapted)
# MAGIC 5. Run notebooks in order (adapted Fabric versions):
# MAGIC    * 00_README (overview – this file)
# MAGIC    * 01_policy_claims_accident.py (ingest policies & claims)
# MAGIC    * 03_iot.py (telematics / simulated streaming)
# MAGIC    * 05_severity_prediction.py or 05b_severity_prediction_silver_fabric.py (ML severity & Silver enrichment)
# MAGIC    * 06_rule.py (rule engine / Gold prep)
# MAGIC 6. Build / adjust Power BI report using Direct Lake semantic model over Gold tables
# MAGIC 7. (Optional) Register ML model in Fabric ML / Azure ML & update inference section
# MAGIC 8. Set up Fabric Pipeline or Scheduled Notebook for production cadence
# MAGIC 
# MAGIC ---
# MAGIC ## Replacing Simulated ML
# MAGIC The severity scoring currently uses a simulated heuristic. To integrate a real model:
# MAGIC * Log & register model via Fabric MLflow (tracking & registry)
# MAGIC * Replace simulated UDF with `mlflow.pyfunc.spark_udf` OR batch Pandas UDF scoring
# MAGIC * Optionally integrate Azure OpenAI Vision for zero‑shot / few‑shot severity estimation & text summary of adjuster notes
# MAGIC * Add monitoring: model version drift, severity distribution shift (Silver vs historical baseline)
# MAGIC 
# MAGIC ---
# MAGIC ## Governance & Compliance Considerations
# MAGIC * Classify PII columns (driver, policyholder) with Purview & apply masking / RLS
# MAGIC * Maintain lineage (Bronze → Silver → Gold) for audit & rate filing transparency
# MAGIC * Capture model explainability artifacts (feature attributions) for dispute resolution
# MAGIC * Encrypt at-rest / in-flight (handled by platform) & enforce least‑privilege Entra roles
# MAGIC 
# MAGIC ---
# MAGIC ## Extensibility Ideas
# MAGIC * Real‑time FNOL (First Notice of Loss) ingestion & immediate triage
# MAGIC * Geospatial enrichment (weather, road conditions, hazard indices)
# MAGIC * Generative AI claim summary + next best action recommendation
# MAGIC * Dynamic reserving model (link severity + coverage specifics)
# MAGIC * Fraud graph features (entity linking across claims, devices, locations)
# MAGIC 
# MAGIC ---
# MAGIC ## Fabric vs Fragmented Stacks (Value Rationale)
# MAGIC | Traditional Fragmented | Fabric Unified |
# MAGIC |------------------------|---------------|
# MAGIC | Multiple storage silos | OneLake single logical data layer |
# MAGIC | ETL handoffs & latency | Direct Lake & shared Delta tables |
# MAGIC | Disconnected ML ops | Inline registry + notebook + pipeline lifecycle |
# MAGIC | BI extracts / copies | Zero-copy analytics (Direct Lake) |
# MAGIC | Complex security overlays | Central governance & lineage |
# MAGIC 
# MAGIC ---
# MAGIC ## Quick Start Checklist
# MAGIC - [ ] Git repo connected in Fabric
# MAGIC - [ ] Lakehouse default set
# MAGIC - [ ] Raw data files available (or generated)
# MAGIC - [ ] Bronze ingestion notebook run
# MAGIC - [ ] Silver severity enrichment successful (chunked incremental path avoids OOM)
# MAGIC - [ ] Gold tables & Power BI model published
# MAGIC - [ ] (Optional) Real model registered & inference validated
# MAGIC - [ ] Pipeline scheduled & monitored
# MAGIC 
# MAGIC ---
# MAGIC ## Disclaimer
# MAGIC This accelerator adaptation for Microsoft Fabric is provided **AS IS** for educational & exploratory purposes. No production warranty or official support. Review licensing in `LICENSE`. Replace or remove synthetic data before using with real customer information.
# MAGIC 
# MAGIC ---
# MAGIC ## Original Attribution
# MAGIC Portions of conceptual diagrams & synthetic dataset structure originate from the original Databricks Smart Claims solution accelerator (Apache 2 / respective licenses). Adapted here for Fabric capability illustration.
# MAGIC 
# MAGIC ---
# MAGIC **Next:** Proceed to the ingestion notebook to begin the pipeline.


