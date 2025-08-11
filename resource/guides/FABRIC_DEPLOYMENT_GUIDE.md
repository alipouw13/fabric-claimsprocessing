# Microsoft Fabric Smart Claims Pipeline Deployment Guide

## Overview

This guide helps you deploy the Smart Claims solution in Microsoft Fabric using a Medallion (bronze → silver → gold) architecture with star‑schema gold materialized views.

## Prerequisites

### Fabric Requirements
- ✅ Microsoft Fabric workspace (F-SKU)
- ✅ Contributor role in the Fabric workspace
- ✅ Lakehouse created for data storage
- ✅ Git integration enabled (optional but recommended)

### Data Requirements
See `resource\data_sources` for all the files below:

- 📄 Claims data (JSON format)
- 📄 Policy data (CSV format)
- 📸 Accident images with metadata
- 📊 Telematics data (Parquet format)

## Architecture Comparison

### Databricks vs Fabric
| Component | Databricks | Microsoft Fabric |
|-----------|------------|------------------|
| **Orchestration** | Workflows/Jobs | Data Pipelines |
| **Compute** | Clusters | Spark Compute |
| **Storage** | DBFS/Delta | OneLake/Delta |
| **Notebooks** | Databricks Notebooks | Fabric Notebooks |
| **DLT** | Delta Live Tables | Manual Delta Operations |
| **Dashboards** | Databricks SQL | Power BI |

## File Structure (Updated Logical Layers)

```
resource/
├── data_sources/
│   ├── Claims/            # JSON claim files
│   ├── Policies/          # CSV policy files
│   ├── Accidents/         # Image files + metadata + image_metadata.csv
│   └── Telematics/        # Parquet files
├── guides/                # deployment and use guides
└── notebooks/             # Imported Fabric notebooks (00–07)
```

## Medallion Architecture
- Bronze: Raw ingested (claims, policies, telematics, accident images, metadata)
- Silver: Cleansed & conformed (joined policy/claim, geocoded, telematics curated, accident severity scored incrementally)
- Gold: Star schema materialized views (dimensions, facts, aggregates) consumed directly by Power BI (Direct Lake)

## Pipeline Architecture (Updated)

```mermaid
graph TD
    A[Raw Data Sources] --> B[01 Source→Bronze]
    B --> C[02 Bronze→Silver]
    C --> D[03 IoT Telematics]
    C --> E[04 Location Enrichment]
    B --> F[05a Images Bronze]
    F --> G[05b Severity Prediction (Incremental)]
    D --> H[06 Rules Engine]
    E --> H
    G --> H
    H --> I[07 Gold Views & Aggregates]
    I --> J[Power BI Star Model]
```

## Notebook Import Order (Updated)
| Order | Notebook File | Purpose |
|-------|---------------|---------|
| 0 | `00_README.py` | Orientation / optional validation step |
| 1 | `01_policy_claims_sourceToBronze.py` | Ingest raw claims & policies to bronze |
| 2 | `02_policy_claims_bronzeToSilver.py` | Conform / cleanse to silver tables |
| 3 | `03_iot.py` | Telematics ingestion & standardization |
| 4 | `04_policy_location.py` | Geocoding enrichment (silver) |
| 5 | `05a_accident_images_bronze_fabric.py` | Image & metadata ingestion to bronze |
| 6 | `05b_severity_prediction_bronzeToSilver.py` | Incremental severity scoring to silver_accident |
| 7 | `06_rules_engine.py` | Business rules producing gold.gold_insights |
| 8 | `07_policy_claims_accident_Goldviews.py` | Create gold dimensions, facts, aggregates |

## Incremental Severity Pipeline Highlights
- Chunks image metadata to avoid OOM
- Maintains progress checkpoint for idempotent restarts
- Writes partitioned `silver.silver_accident` (e.g. by ingestion or date)
- Skips already processed images (idempotent)

## Deployment Steps (Condensed)
1. Create Lakehouse & upload source data.
2. Import notebooks (order above).
3. Execute 01 → 02 sequentially to build silver baseline.
4. Run 05a then 05b (can run 03 & 04 in parallel after 02).
5. Run 06 after 03,04,05b complete.
6. Run 07 to materialize gold star schema & aggregates.
7. Connect Power BI to gold views only.

## Gold Star Schema Objects
Dimensions: `gold.dim_date`, `gold.dim_claim`, `gold.dim_policy`, `gold.dim_vehicle`, `gold.dim_location`, `gold.dim_risk`
Facts: `gold.fact_claims`, `gold.fact_telematics`, `gold.fact_accident_severity`, `gold.fact_rules`, `gold.fact_smart_claims`
Aggregates: `gold.v_claims_summary_by_risk`, `gold.v_smart_claims_dashboard`

## Power BI Integration
Recommended import (Direct Lake):
- Dimensions: all `gold.dim_*`
- Facts: core facts above (choose only needed for model to avoid bloat)
- Aggregates: `gold.v_smart_claims_dashboard` for quick KPIs

Sample DAX (using gold schema):
```dax
Total Claim Amount = SUM(gold.fact_claims[claim_amount])
Avg Severity Score = AVERAGE(gold.fact_accident_severity[severity_score])
High Severity Claims = CALCULATE(COUNTROWS(gold.fact_accident_severity), gold.fact_accident_severity[severity_category] = "High")
Auto Approved % = DIVIDE(
    CALCULATE(COUNTROWS(gold.fact_rules), gold.fact_rules[rule_outcome] = "AUTO_APPROVE"),
    COUNTROWS(gold.fact_rules)
)
Claim Processing SLA Breach % = DIVIDE(
    CALCULATE(COUNTROWS(gold.fact_claims), gold.fact_claims[processing_time_hours] > 48),
    COUNTROWS(gold.fact_claims)
)
```

## Pipeline Activity JSON (Illustrative)
```json
[
  {"name": "01_source_to_bronze", "type": "Notebook"},
  {"name": "02_bronze_to_silver", "type": "Notebook", "dependsOn": ["01_source_to_bronze"]},
  {"name": "03_iot_telematics", "type": "Notebook", "dependsOn": ["02_bronze_to_silver"]},
  {"name": "04_location_enrichment", "type": "Notebook", "dependsOn": ["02_bronze_to_silver"]},
  {"name": "05a_images_bronze_ingest", "type": "Notebook", "dependsOn": ["01_source_to_bronze"]},
  {"name": "05b_severity_prediction", "type": "Notebook", "dependsOn": ["05a_images_bronze_ingest"]},
  {"name": "06_rules_engine", "type": "Notebook", "dependsOn": ["03_iot_telematics", "04_location_enrichment", "05b_severity_prediction"]},
  {"name": "07_gold_views_materialization", "type": "Notebook", "dependsOn": ["06_rules_engine"]}
]
```

## Success Criteria
- All 9 notebooks executed in order with dependencies satisfied
- Bronze, silver, and gold schemas populated
- Incremental severity pipeline completed without reprocessing duplicates
- Gold star schema & aggregates queryable (`SHOW TABLES IN gold`)
- Power BI model built only on gold layer
- Rules engine outputs reflected in gold.fact_rules & gold.gold_insights

## Additional Troubleshooting
| Issue | Symptom | Resolution |
|-------|---------|-----------|
| Duplicate severity rows | Increased row count each rerun | Verify checkpoint path & id column de-dup logic in 05b |
| Missing gold views | Power BI cannot see gold tables | Re‑run 07 or confirm materialized view creation succeeded |
| Slow Power BI refresh | High Direct Lake latency | Limit imported columns; ensure star schema relationships; Clear PBI Desktop cache |

*Guide aligned to updated notebook set, medallion layering, and gold star schema.*
