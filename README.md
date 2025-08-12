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
- Purview (optional): data catalog, lineage, classification

## 3. Repository Structure
```
root/
  notebooks/
    00_README.py                              Fabric-oriented overview
    01_policy_claims_sourceToBronze.py        Ingest raw policy & claims sources to Bronze
    02_policy_claims_bronzeToSilver.py        Bronze → Silver normalization & conformance
    03_iot.py                                 Telematics / IoT ingestion simulation (optional streaming stub)
    04_policy_location.py                     Geospatial enrichment (zipcode → lat/long)
    05a_accident_images_sourceToBronze.py     Accident image metadata & content ingestion to Bronze (preferred)
    05a_accident_images_bronze_fabric.py      Alternative 05a implementation
    05_import_model.py                        Import/prepare ML model artifacts for severity scoring
    05b_severity_prediction_bronzeToSilver.py Chunked ML severity scoring & Silver accident table build
    06_rules_engine.py                        Rules engine (validations; writes gold.gold_insights)
    07_policy_claims_accident_Goldviews.py    Build Gold dimensions/facts and reporting views (tables + views)
  resource/
    data_sources/                             Synthetic raw datasets (policies, claims, telematics, images)
    guides/                                   Guides for Fabric deployment, Power BI, AI Foundry integration
    images/                                   Visualization assets
    pbi-report/                                Power BI Direct Lake report project (.pbip)
  FABRIC_PIPELINE_CONFIG.py                   Pipeline config to run the notebooks
  README.md                                   (this file)
```

## 4. Data Layers (Medallion Pattern)
| Layer  | Purpose | Sample Tables |
|--------|---------|---------------|
| Bronze | Raw landed, minimal schema normalization | bronze_claim, bronze_policy, bronze_accident, bronze_telematics, bronze_images |
| Silver | Cleansed, conformed, enriched (ML severity, joins) | silver_policy, silver_claim, silver_accident, silver_claim_policy_location, silver_smart_claims_analytics |
| Gold   | Governed star schema dimensions/facts + views | dim_date, dim_claim, dim_policy, dim_vehicle, dim_location, dim_risk, fact_claims, fact_telematics, fact_accident_severity, fact_rules, fact_smart_claims, v_claims_summary_by_risk |

Governance artifacts (Gold):
- gold.constraint_audit – metrics validating PK uniqueness and orphan FKs
- gold.key_metadata – catalog of PK/FK definitions

Key surrogate keys:
- location_id (GUID) in dim_location and referenced by facts
- risk_key (surrogate built from severity_category|risk_category|processing_flag|speed_risk_indicator) in dim_risk and referenced by facts

## 5. Ingestion & Processing Workflow
1. Run 01_policy_claims_sourceToBronze.py to land raw policy & claim data into Bronze Delta tables.
2. (Optional) Run 03_iot.py for telematics ingestion (batch or simulated stream stub).
3. Execute 02_policy_claims_bronzeToSilver.py to standardize & enrich core entities into Silver.
4. Run 04_policy_location.py for location (geo) enrichment.
5. Run 05a_accident_images_sourceToBronze.py to ingest accident image metadata/binaries to Bronze. (Alternative: 05a_accident_images_bronze_fabric.py)
6. Run 05_import_model.py to import/prepare ML artifacts.
7. Execute 05b_severity_prediction_bronzeToSilver.py for incremental severity scoring & Silver accident enrichment.
8. Apply rules via 06_rules_engine.py (produces gold.gold_insights when present).
9. Build Gold dimensions, facts, and views with 07_policy_claims_accident_Goldviews.py.
10. Publish a Power BI model (Direct Lake) pointing to Gold tables/views.
11. Schedule notebooks in a Fabric Pipeline; add monitoring & alerting (see FABRIC_PIPELINE_CONFIG.py).

## 6. ML & Rules Enrichment
Severity Model:
- Replaceable with MLflow registered model or Azure OpenAI Vision scenario.
- Chunk-based inference avoids memory pressure (incremental writes with validation).

Rules Engine:
- Validity checks (date, amount), processing flags, and quality indicators.
- Outputs gold.gold_insights consumed by Gold fact_rules when present.

## 7. Gold Semantic Layer
Physical Delta tables (not just views):
- Dimensions: gold.dim_date, gold.dim_claim, gold.dim_policy, gold.dim_vehicle, gold.dim_location (PK: location_id GUID), gold.dim_risk (PK: risk_key)
- Facts: gold.fact_claims, gold.fact_telematics, gold.fact_accident_severity, gold.fact_rules (conditional), gold.fact_smart_claims (wide, for prototyping)
- Views: gold.v_claims_summary_by_risk, gold.v_smart_claims_dashboard

Recommended Power BI model:
- Use star schema as defined in [guides/POWERBI_DASHBOARD_GUIDE.md](https://github.com/alipouw13/fabric-claimsprocessing/blob/main/resource/guides/POWER_BI_DASHBOARD_GUIDE.md)
- Use fact_telematics and fact_accident_severity for specialized analysis; keep fact_smart_claims hidden after modeling is complete.

## 8. Reporting (Power BI / SQL)
Two consumption patterns:
- Direct Lake on Gold tables/views (recommended)
- Fabric SQL DB replication (optional) for enterprise semantics/RLS/API reuse

Note: A ready-to-open Power BI project is provided under `pbi-report/` (.pbip). Open it in Power BI Desktop to load the Direct Lake model and report.

Power BI Dataset Modeling Steps:
1. Connect to Lakehouse (Direct Lake) and select Gold tables/views.
2. Import dimensions first, then facts.
3. Define relationships as above (use location_id and risk_key).
4. Mark dim_date as date table; hide raw key columns on facts after relationships.
5. Add DAX measures (examples [guides/POWERBI_DASHBOARD_GUIDE.md](https://github.com/alipouw13/fabric-claimsprocessing/blob/main/resource/guides/POWER_BI_DASHBOARD_GUIDE.md)).

Core DAX Measures (examples):
```
Total Claims = DISTINCTCOUNT(fact_claims[claim_no])
Total Claim Amount = SUM(fact_claims[claim_amount_total])
Avg Claim Amount = DIVIDE([Total Claim Amount],[Total Claims])
High Severity Claims = CALCULATE([Total Claims], fact_claims[severity_category] IN {"High","Medium-High"})
High Severity % = DIVIDE([High Severity Claims],[Total Claims])
Risk Weighted Amount = SUMX(fact_claims, fact_claims[claim_amount_total] * fact_claims[severity])
Avg Telematics Speed = AVERAGE(fact_telematics[telematics_speed])
```

Suggested visuals:
- KPI Cards: Total Claims, Total Claim Amount, High Severity %, Loss Ratio (with premium from dim_policy)
- Column: Claim Amount by Risk Category
- Line: Claim Amount over Month (dim_date)
- Scatter: Severity vs Telematics Speed (use fact_telematics)
- Map: Claims by location_id (dim_location)

## 9. Governance & Security
- gold.constraint_audit and gold.key_metadata to monitor PK/FK health
- Purview for catalog/lineage/classification
- Optional RLS in semantic model (apply on dimensions)

## 10. Getting Started (Fabric)
1. Create a Fabric Workspace (enable Lakehouse & Data Science experiences)
2. Connect Git repo (this repository)
3. Create Lakehouse (set as default)
4. Place raw data under Files/resource/data_sources/
5. Run notebooks sequentially (01 -> 04)
6. Confirm Bronze table creation (SHOW TABLES or Lakehouse UI)
7. Execute 05a_accident_images_sourceToBronze.py then 05_import_model.py and 05b_severity_prediction_bronzeToSilver.py
8. Run 06_rules_engine.py to apply rules & persist gold_insights
9. Run 07_policy_claims_accident_Goldviews.py to build Gold model
10. Build Power BI report (Direct Lake) pointing to Gold
11. Configure Pipeline schedule and alerts (see guides)

## 11. Operational Considerations
| Aspect | Approach |
|--------|----------|
| Incremental Resume | Progress checkpoints for chunked severity writes |
| Schema Evolution | mergeSchema on append where safe |
| Performance | Partition pruning; avoid wide table for primary reporting |
| Cost Efficiency | Zero-copy Direct Lake; selective SQL DB serving |
| Observability | constraint_audit metrics; logging for pipeline stages |

## 12. Environment Variables / Secrets (if extended)
| Key | Purpose |
|-----|---------|
| AZURE_AI_FOUNDRY_ENDPOINT | Vision / LLM inference endpoint |
| AZURE_AI_FOUNDRY_KEY | API key for model inference |

(Use Fabric Workspace credentials / Key Vault connectors.)

## 13. Limitations / Notes
- Synthetic dataset (non-production) – replace with governed real data
- Image binary content excluded from Silver by default (configurable)
- Real-time event ingestion not implemented (stub provided)

## 14. License & Attribution
Original conceptual assets derived from prior open accelerator work; adapted for Microsoft Fabric architecture. Third-party libraries retain their respective licenses.

## 15. Support
Community / best-effort only. Potential file issues in repo. No formal SLA.
