# Smart Claims Processing on Microsoft Fabric

> NOTE: This repository is an adaptation of the open accelerator at https://github.com/databricks-industry-solutions/smart-claims. It reuses the same synthetic insurance data and mirrors the original notebook flow (ingestion → enrichment → ML severity → rules → reporting) while refactoring code, schemas, and serving artifacts for Microsoft Fabric (Lakehouse medallion layers, schema-qualified bronze/silver/gold tables, gold star-schema materialized views, and Direct Lake Power BI guidance).

## 1. Purpose & Business Context
Modern Property & Casualty insurers need to simultaneously:
- Reduce claims cycle time & operating expense
- Contain fraud & leakage early
- Provide explainable, data-driven triage to adjusters
- Personalize engagement and improve customer retention

This repository delivers a reference implementation of an end-to-end Smart Claims pipeline on Microsoft Fabric using a Lakehouse + SQL DB pattern. It ingests heterogeneous insurance data (policies, claims, telematics, images metadata) from Azure Storage, curates it through medallion layers (Bronze → Silver → Gold), enriches with ML severity scoring & rule-based quality flags, and exposes analytics & KPIs through Power BI Direct Lake and/or a Fabric SQL Database.

## 2. Target Architecture (High-Level)
Data Flow: Azure Storage (landing) → Fabric Lakehouse (Bronze/Silver/Gold Delta) → Fabric SQL DB (optional serving) → Power BI (Direct Lake or SQL) → Downstream operational feedback.

```
+-------------------+        +------------------+        +------------------+        +-----------------+
|  Azure Storage    | -----> |  Fabric Ingest   | -----> |  Lakehouse       | -----> |  Gold / Serving |
|  (Raw Files)      |  ADLS  |  (Pipelines /    |  Spark |  Bronze/Silver/  |  Delta |  (SQL DB /      |
|   - policies.csv  |        |   Notebooks)     |        |   Gold Tables    |        |   Semantic / BI)|
|   - claims JSON   |        +------------------+        +--------+---------+        +---------+-------+
|   - telematics    |                                             |                          |
|   - images (bin)  |                                             |                          |
+---------+---------+                                             v                          v
          |                                                ML / Rules Enrichment       Power BI Direct Lake
          |                                                (Severity, Quality)         Dashboards / Reports
          v
  Optional Real-Time Stream (Event / Telematics) --> KQL DB --> Join into Silver
```

Key Components:
- OneLake / Lakehouse: unified storage for structured & semi-structured data (Delta format)
- Spark Notebooks: ingestion, transformation, ML inference, rule engine logic
- SQL DB (optional): serving layer for downstream transactional or API-based consumption
- Power BI Direct Lake: zero-copy BI on Gold Delta tables
- MLflow (optional): model registry for severity model
- Azure OpenAI / Vision (extensible): advanced damage assessment & summarization
- Purview (Governance): data catalog, lineage, classification

## 3. Repository Structure
```
root/
  notebooks/
    00_README.py                         Fabric-oriented overview
    01_policy_claims_sourceToBronze.py    Ingest raw policy & claims sources to Bronze
    02_policy_claims_bronzeToSilver.py    Bronze → Silver normalization & conformance
    03_iot.py                             Telematics / IoT ingestion simulation (optional streaming stub)
    04_policy_location.py                 Geospatial enrichment (zipcode → lat/long)
    05_import_model.py                    Utility to import / register ML model artifact
    05a_accident_images_bronze_fabric.py  Accident image metadata & content ingestion to Bronze
    05b_severity_prediction_silver_fabric.py  Chunked ML severity scoring & Silver accident table build
    06_rule.py                            Base rule application / transformations
    06_rules_engine.py                    Extended rule engine (composite risk & quality scoring)
    07_policy_claims_accident_views.sql   SQL views / serving layer definitions (joins & curated views)
  resource/
    data_sources/                         Synthetic raw datasets (policies, claims, telematics, images)
    images/                               Visualization assets
    Model/                                Placeholder ML model artifacts
  docs/                                   Additional documentation / PDFs
  setup/                                  Environment / model initialization scripts
  README.md                               (this file)
```

## 4. Data Layers (Medallion Pattern)
| Layer  | Purpose | Sample Tables |
|--------|---------|---------------|
| Bronze | Raw landed, minimal schema normalization | bronze_claim, bronze_policy, bronze_accident, bronze_telematics |
| Silver | Cleansed, conformed, enriched (ML severity, rule quality, joins) | silver_policy, silver_claim, silver_accident, silver_claim_policy_accident, silver_claim_policy_telematics |
| Gold   | Aggregated KPIs, risk indicators, fraud & operational metrics | gold_insights, claim_rules |

Partitioning & Incremental Strategy:
- Large image-related tables processed incrementally (chunked append) to avoid memory pressure.
- Partitioning by `partition_date` for cost-efficient pruning in query workloads.

## 5. Ingestion & Processing Workflow
1. Run `01_policy_claims_sourceToBronze.py` to land raw policy & claim data into Bronze Delta tables.
2. (Optional) Run `05a_accident_images_bronze_fabric.py` to ingest accident image metadata/binaries to Bronze.
3. (Optional / Sim) Run `03_iot.py` for telematics ingestion (batch or simulated stream stub).
4. Execute `02_policy_claims_bronzeToSilver.py` to standardize & enrich core policy/claim entities into Silver.
5. Run `04_policy_location.py` for location (geo) enrichment.
6. Run `05_import_model.py` if deploying a real severity model (MLflow import / registration).
7. Execute `05b_severity_prediction_silver_fabric.py` for incremental severity scoring & Silver accident enrichment.
8. Apply rules via `06_rules_engine.py` (produces flags / risk metrics).
9. Create / refresh analytic views with `07_policy_claims_accident_views.sql` (serving / Gold alignment).
10. Publish Power BI model (Direct Lake) or replicate selected views to Fabric SQL DB.
11. Schedule notebooks / SQL in a Fabric Pipeline; add monitoring & alerting.

## 6. ML & Rules Enrichment
Severity Model:
- Simulated predictions now; replace with MLflow registered model or Azure OpenAI Vision scenario.
- Chunk-based inference: prevents out-of-memory conditions by splitting large image sets.

Rule Categories (illustrative):
- Coverage Period Validity
- Severity Alignment (reported vs predicted)
- Location Consistency (telematics vs claimed coordinates)
- Speed / Behavioral Anomalies
- Policy Limit Breach Flags

Outputs:
- `severity`, `severity_category`, `high_severity_flag`
- `data_quality_score`
- `rule_flags` (extendable structure)

## 7. Gold KPIs & Metrics (Examples)
| KPI | Description |
|-----|-------------|
| Loss Ratio Components | Paid + LAE vs Earned Premium segments |
| Severity Mix | Distribution of categorical severity bands |
| High-Risk Claim Rate | Percentage flagged with multiple anomalies |
| Fraud Suspect Count | Claims failing selected rule thresholds |
| Average Cycle Time (extendable) | Requires additional timing fields |
| Claim Amount per Risk Band | SUM(claim_amount_total) by risk_category |
| Speed-Severity Correlation | Avg severity vs telematics_speed buckets |

### Gold Semantic Layer Objects
Materialized Views (schema `gold`):
- Dimensions: `gold.dim_date`, `gold.dim_claim`, `gold.dim_policy`, `gold.dim_vehicle`, `gold.dim_location`, `gold.dim_risk`
- Facts: `gold.fact_claims`, `gold.fact_telematics`, `gold.fact_accident_severity`, `gold.fact_rules`, `gold.fact_smart_claims` (wide) 
- Aggregations: `gold.v_claims_summary_by_risk`, `gold.v_smart_claims_dashboard`

Recommended Power BI model: use star schema (fact_claims central) with conformed keys (claim_no, policy_no, chassis_no, ZIP_CODE, severity_category). Hide wide table if dimensional model adopted.

## 8. Reporting (Power BI / SQL)
Two consumption patterns:
- Direct Lake on Gold materialized views (reduced latency, star schema ready)
- Fabric SQL DB replication (optional) for enterprise semantic models, RLS, API reuse

### Power BI Dataset Modeling Steps
1. Connect to Lakehouse (Direct Lake) and select `gold` schema views.
2. Import dimension views first, then fact views.
3. Define relationships:
   - fact_claims.claim_no  → dim_claim.claim_no (1:* set dim to single)
   - fact_claims.policy_no → dim_policy.policy_no
   - fact_claims.chassis_no → dim_vehicle.chassis_no
   - fact_claims.severity_category → dim_risk.severity_category
   - fact_claims.claim_date → dim_date.date_key
   - (Optional) ZIP_CODE → dim_location.ZIP_CODE
4. Hide surrogate or duplicate columns not needed in visuals.
5. Mark `dim_date` as date table.
6. Add DAX measures (see below).

### Core DAX Measures (Updated)
```
Total Claims = COUNT(fact_claims[claim_no])
Total Claim Amount = SUM(fact_claims[claim_amount_total])
Avg Claim Amount = DIVIDE([Total Claim Amount],[Total Claims])
High Severity Claims = CALCULATE([Total Claims], fact_claims[severity_category] IN {"High","Medium-High"})
High Severity % = DIVIDE([High Severity Claims],[Total Claims])
Total Exposure = SUM(fact_claims[sum_insured])
Severity Index = AVERAGE(fact_claims[severity])
Risk Weighted Amount = SUMX(fact_claims, fact_claims[claim_amount_total] * fact_claims[severity])
Release Funds Count = COUNTROWS(FILTER(fact_rules, fact_rules[release_funds] = "release funds"))
Release Funds % = DIVIDE([Release Funds Count],[Total Claims])
Avg Telematics Speed = AVERAGE(fact_telematics[telematics_speed])
Speed Risk Flagged Claims = CALCULATE([Total Claims], fact_claims[speed_risk_indicator] <> "NORMAL_RISK")
Speed Risk % = DIVIDE([Speed Risk Flagged Claims],[Total Claims])
Severity vs Speed Corr (Approx) = 
    VAR t = SUMMARIZE(fact_claims, fact_claims[claim_no], "sev", AVERAGE(fact_claims[severity]), "spd", AVERAGE(fact_claims[telematics_speed]))
    RETURN 
    GENERATECOLUMNS( { (CORRX(t[sev], t[spd])) } ) // placeholder if custom correlation measure pattern used
```
(Adjust correlation pattern or use a disconnected calculation table; DAX does not expose CORR natively—implement via statistical pattern if needed.)

### Suggested Visuals
- KPI Cards: Total Claims, Total Claim Amount, High Severity %, Release Funds %
- Bar: Claim Amount by Risk Category
- Line: Severity Index over Month (use dim_date)
- Scatter: Telematics Speed vs Severity (risk coloring)
- Map: Claim Count by ZIP_CODE / BOROUGH
- Matrix: Processing Flag x Risk Category (claim_count, avg_claim_amount)

### Aggregation Strategy
Leverage `gold.v_claims_summary_by_risk` for high-level cards; Direct Lake will automatically hit summarized view when visual grain matches.

## 9. Governance & Security
- Column classification (PII: driver, insured contact) via Purview
- Row / object-level security (e.g., region-based adjuster segregation) enforced in semantic model or SQL DB
- Lineage tracked from Bronze input file → Gold KPI (audit, compliance, model transparency)
- Versioned ML models with metadata (who deployed, when, performance snapshot)

## 10. Extensibility Roadmap
| Enhancement | Value |
|-------------|-------|
| Real-Time Telematics (Event Stream + KQL) | Sub-second anomaly trigger pipeline |
| Generative AI Claim Summaries | Adjuster productivity, consistent narratives |
| Graph / Network Fraud Features | Link analysis for organized fraud rings |
| Dynamic Reserving Model | Improved accuracy in early lifecycle |
| External Data Enrichment (Weather / Geo Risk) | Contextual risk scoring |

## 11. Getting Started (Fabric)
1. Create a Fabric Workspace (enable Lakehouse & Data Science experiences)
2. Connect Git repo (this repository)
3. Create Lakehouse (set as default)
4. Place raw data in `Files/resource/data_sources/` (or adapt paths)
5. Run notebooks sequentially (00_README optional overview first)
6. Confirm Bronze table creation (`spark.sql("SHOW TABLES")` or Lakehouse UI)
7. Execute `05b_severity_prediction_silver_fabric.py` – verify incremental severity population
8. Run `06_rule.py` to apply rules & produce Gold insights
9. Build Power BI report (Direct Lake) pointing to Gold layer
10. (Optional) Register real model & swap inference logic
11. Configure Pipeline schedule (daily/hourly) & alerts

## 12. Operational Considerations
| Aspect | Approach |
|--------|----------|
| Incremental Resume | Progress checkpoint file for chunked severity writes |
| Schema Evolution | `mergeSchema` on append (controlled) |
| Performance | Partition pruning, OPTIMIZE (when supported), reduced image payload in Silver |
| Cost Efficiency | Avoid duplicate copies (Direct Lake), selective SQL DB serving |
| Observability | Add logging & (future) metrics table for pipeline SLA tracking |

## 13. Environment Variables / Secrets (if extended)
| Key | Purpose |
|-----|---------|
| AZURE_AI_FOUNDRY_ENDPOINT | Vision / LLM inference endpoint |
| AZURE_AI_FOUNDRY_KEY | API key for model inference |

(Integrate via Fabric Workspace credentials / Key Vault connector.)

## 14. Replacing the Simulated Model
Steps:
1. Log model run with MLflow in Fabric notebook
2. Register to model registry (e.g., `damage_severity_model`)
3. Update config: `use_simulated_predictions=False`
4. Use `mlflow.pyfunc.load_model()` (batch) or `spark_udf` for distributed inference
5. Add monitoring notebook for drift (compare severity distribution month over month)

## 15. Limitations / Notes
- Synthetic dataset (non-production) – replace with governed real data
- Image binary handling optimized by excluding content from Silver (configurable)
- No real-time event ingestion implemented (stub for future extension)

## 16. License & Attribution
Original conceptual assets derived from prior open accelerator work; adapted for Microsoft Fabric architecture. See `LICENSE` for details. Third-party libraries retain their respective licenses.

## 17. Support
Community / best-effort only. File Issues in repo. No formal SLA.

---
**Next Step:** Run the ingestion notebook and validate Bronze tables before ML severity enrichment.
