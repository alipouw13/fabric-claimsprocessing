# Smart Claims Processing on Microsoft Fabric

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

## 8. Reporting (Power BI / SQL)
Two consumption patterns:
- Direct Lake on Gold Delta tables for interactive dashboards (single copy, low latency)
- Fabric SQL DB (ELT push) for standardized semantic & operational APIs

Sample Dashboards:
- Loss Summary: trend lines, severity mix, geographic context
- Claims Investigation: per-claim drill (images, telematics path, rule exceptions, ML severity vs reported)

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
