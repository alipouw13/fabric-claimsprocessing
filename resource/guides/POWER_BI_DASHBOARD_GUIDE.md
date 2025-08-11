# Smart Claims Power BI Dashboard Guide (Star / Snowflake Schema – Natural Key, GUID Location & Audit Enhanced)

This guide aligns with the latest gold layer where dimensions and facts are **physical Delta tables** (not just views), natural keys are logically enforced via deduping, a surrogate GUID `location_id` replaces previous composite location keys, and two governance artifacts exist:
- `gold.constraint_audit` – metrics validating PK uniqueness & orphan FKs
- `gold.key_metadata` – catalog of primary & foreign key definitions

---
## 1. Data Sources (Gold Layer Tables & Views)
### Dimension Tables (PKs)
- `gold.dim_date` (PK: date_key)
- `gold.dim_claim` (PK: claim_no – latest snapshot per claim)
- `gold.dim_policy` (PK: policy_no – latest snapshot)
- `gold.dim_vehicle` (PK: chassis_no – latest snapshot)
- `gold.dim_location` (PK: location_id – deterministic GUID from zip/lat/lon)
- `gold.dim_risk` (PK: risk_key – surrogate composed as severity_category|risk_category|processing_flag|speed_risk_indicator; natural components retained as attributes)

### Fact Tables
- `gold.fact_claims` (grain: claim; minimal core numeric & classification attributes + location_id FK)
- `gold.fact_telematics` (grain: claim_no + chassis_no; telematics summary + location_id)
- `gold.fact_accident_severity` (grain: claim snapshot for severity lineage + location_id)
- `gold.fact_rules` (conditional – populated when rules engine runs)
- `gold.fact_smart_claims` (wide denormalized analytics table including telematics & location_id; for prototyping only)

### Reporting Views
- `gold.v_claims_summary_by_risk`
- `gold.v_smart_claims_dashboard`

### Governance Tables
- `gold.constraint_audit`
- `gold.key_metadata`

> All dimension tables (except dim_risk) are deduped using ROW_NUMBER() over the natural key ordered by `analytics_created_timestamp` to retain latest state.

---
## 2. Star / Optional Snowflake Strategy
`fact_claims` is the central fact. Telematics, rules, severity history extend model as secondary facts. Location now uses a **single GUID key** (`location_id`). Risk now has a persisted **surrogate key** (`risk_key`) replacing prior purely composite relationship logic.

### Relationship Matrix (Current Model)
| From Dimension | To Fact                | Key Mapping                                   | Cardinality |
|----------------|------------------------|-----------------------------------------------|-------------|
| dim_claim      | fact_claims            | claim_no                                      | 1:*         |
| dim_policy     | fact_claims            | policy_no                                     | 1:*         |
| dim_vehicle    | fact_claims            | chassis_no                                    | 1:*         |
| dim_date       | fact_claims            | date_key → claim_date                         | 1:* (mark dim_date) |
| dim_location   | fact_claims            | location_id                                   | 1:*         |
| dim_risk       | fact_claims            | risk_key                                      | 1:*         |
| dim_claim      | fact_telematics        | claim_no                                      | 1:*         |
| dim_vehicle    | fact_telematics        | chassis_no                                    | 1:*         |
| dim_location   | fact_telematics        | location_id                                   | 1:*         |
| dim_risk       | fact_telematics        | risk_key                                      | 1:*         |
| dim_claim      | fact_accident_severity | claim_no                                      | 1:*         |
| dim_vehicle    | fact_accident_severity | chassis_no                                    | 1:*         |
| dim_location   | fact_accident_severity | location_id                                   | 1:*         |
| dim_risk       | fact_accident_severity | risk_key                                      | 1:*         |
| dim_claim      | fact_rules             | claim_no                                      | 1:*         |
| dim_policy     | fact_rules             | policy_no                                     | 1:*         |
| dim_vehicle    | fact_rules             | chassis_no                                    | 1:*         |

### Risk Key Construction
Surrogate `risk_key` = `severity_category & "|" & risk_category & "|" & processing_flag & "|" & speed_risk_indicator`.
Persisted in `dim_risk` and each fact (*except* `fact_rules`, which currently lacks risk attributes). If needed in `fact_rules`, add source attributes upstream and derive `risk_key` similarly.

> Previous optional surrogate section removed – key now physical.

---
## 3. Primary / Foreign Key Governance
Validation logic (no physical constraints) enforced during build; results written to:
- `gold.constraint_audit`: metrics: rows, distinct_pk, duplicate_pk_rows, orphan_fk_<col>_to_<dim>
- `gold.key_metadata`: rows documenting PK and FK definitions (human readable)

Use these tables to drive a Data Quality page in Power BI.

Example expected metrics (ideal state):
| entity         | metric                                 | value |
|----------------|-----------------------------------------|-------|
| dim_policy     | duplicate_pk_rows                      | 0     |
| fact_claims    | orphan_fk_claim_no_to_dim_claim        | 0     |
| fact_claims    | orphan_fk_location_id_to_dim_location  | 0     |
| dim_risk      | duplicate_pk_rows                      | 0     |
| fact_claims    | orphan_fk_risk_key_to_dim_risk         | 0     |
| fact_telematics| orphan_fk_risk_key_to_dim_risk         | 0     |
| fact_accident_severity | orphan_fk_risk_key_to_dim_risk | 0     |

Monitor: Any non‑zero orphan or duplicate metric should trigger pipeline investigation.

---
## 4. Load Order
1. Dimensions: date → claim → policy → vehicle → location → risk
2. Facts: claims → telematics → accident_severity → rules (if present) → smart_claims (optional)
3. Governance: constraint_audit, key_metadata (hide both)
4. Create optional surrogate keys (Risk Key) before relationships
5. Mark `dim_date[date_key]` as date table

---
## 5. Calculated Columns (Minimal)
Keep slim; prefer measures.
```DAX
-- Severity band (dim_claim)
Severity Band = SWITCH(TRUE(),
  dim_claim[severity] >= 0.8, "High",
  dim_claim[severity] >= 0.6, "Medium-High",
  dim_claim[severity] >= 0.4, "Medium",
  dim_claim[severity] >= 0.2, "Low-Medium",
  "Low")

-- Driver age bucket (dim_policy)
Driver Age Bucket = SWITCH(TRUE(),
  dim_policy[driver_age] < 25, "<25",
  dim_policy[driver_age] < 35, "25-34",
  dim_policy[driver_age] < 50, "35-49",
  dim_policy[driver_age] < 65, "50-64",
  "65+" )
```

No location surrogate needed – `location_id` is already a GUID.

---
## 6. Measure Design
Organize in folders: Volume, Financial, Severity, Risk, Telematics, Rules, Time Intelligence, Quality.
Use `DIVIDE` and variables (`VAR`) for clarity.

### 6.1 Core Measures (fact_claims)
```DAX
-- Volume
Total Claims = DISTINCTCOUNT(fact_claims[claim_no])
Total Policies = DISTINCTCOUNT(fact_claims[policy_no])
Total Vehicles = DISTINCTCOUNT(fact_claims[chassis_no])
Distinct Locations = DISTINCTCOUNT(fact_claims[location_id])

-- Financial
Total Claim Amount = SUM(fact_claims[claim_amount_total])
Vehicle Claim Amount = SUM(fact_claims[claim_amount_vehicle])
Injury Claim Amount = SUM(fact_claims[claim_amount_injury])
Property Claim Amount = SUM(fact_claims[claim_amount_property])
Avg Claim Amount = DIVIDE([Total Claim Amount],[Total Claims])
Exposure (Sum Insured) = SUM(fact_claims[sum_insured])

-- Severity / Risk
Average Severity = AVERAGE(fact_claims[severity])
High Severity Claims = CALCULATE([Total Claims], fact_claims[severity_category] IN {"High","Medium-High"})
High Severity % = DIVIDE([High Severity Claims],[Total Claims])
Risk Weighted Amount = SUMX(fact_claims, fact_claims[claim_amount_total] * fact_claims[severity])

-- Processing
Processing Review Claims = CALCULATE([Total Claims], fact_claims[processing_flag] = "REVIEW_REQUIRED")
Processing Review % = DIVIDE([Processing Review Claims],[Total Claims])
Total Loss Candidates = CALCULATE([Total Claims], fact_claims[processing_flag] = "TOTAL_LOSS_CANDIDATE")

-- Location
Claims per Location = DIVIDE([Total Claims], DISTINCTCOUNT(fact_claims[location_id]))
```

### 6.2 Telematics (Choose Source)
Preferred: `fact_telematics` (clean separation). Quick prototyping: `fact_smart_claims`.
```DAX
Average Telematics Speed = AVERAGE(fact_telematics[telematics_speed])
Speed StdDev = AVERAGE(fact_telematics[telematics_speed_std])
High Speed Risk Claims = CALCULATE([Total Claims], fact_smart_claims[speed_risk_indicator] IN {"HIGH_SPEED_SEVERE","MODERATE_SPEED_RISK"})
High Speed Risk % = DIVIDE([High Speed Risk Claims],[Total Claims])
```

### 6.3 Rules (Requires fact_rules)
```DAX
Valid Policy Claims = CALCULATE([Total Claims], fact_rules[valid_date] = "VALID")
Valid Policy % = DIVIDE([Valid Policy Claims],[Total Claims])
Valid Amount Claims = CALCULATE([Total Claims], fact_rules[valid_amount] = "claim value in the range of premium")
Valid Amount % = DIVIDE([Valid Amount Claims],[Total Claims])
```

### 6.4 Premium & Ratios
```DAX
Premium Amount = SUM(dim_policy[premium])
Loss Ratio = DIVIDE([Total Claim Amount],[Premium Amount])
Claim Frequency = DIVIDE([Total Claims],[Total Policies])
Avg Accident-Event Distance (Miles) = AVERAGE(fact_claims[accident_telematics_distance_miles])
```

### 6.5 Time Intelligence (dim_date required)
```DAX
Total Claim Amount PY = CALCULATE([Total Claim Amount], DATEADD(dim_date[date_key], -1, YEAR))
YoY Claim Amount % = VAR Curr=[Total Claim Amount] VAR Prev=[Total Claim Amount PY] RETURN DIVIDE(Curr-Prev, Prev)
```

### 6.6 Data Quality (constraint_audit)
```DAX
Orphan Claim FK Count = 
VAR v = CALCULATE(MAX(constraint_audit[value]), constraint_audit[entity]="fact_claims", constraint_audit[metric]="orphan_fk_claim_no_to_dim_claim")
RETURN VALUE(v)

Orphan Location FK Count = 
VAR v = CALCULATE(MAX(constraint_audit[value]), constraint_audit[entity]="fact_claims", constraint_audit[metric]="orphan_fk_location_id_to_dim_location")
RETURN VALUE(v)

Duplicate dim_claim PK Rows = 
VAR v = CALCULATE(MAX(constraint_audit[value]), constraint_audit[entity]="dim_claim", constraint_audit[metric]="duplicate_pk_rows")
RETURN VALUE(v)

Data Quality OK = IF([Orphan Claim FK Count]=0 && [Orphan Location FK Count]=0 && [Duplicate dim_claim PK Rows]=0, "OK", "ISSUES")
```

### 6.7 Risk Dimension
```DAX
Distinct Risk Profiles = DISTINCTCOUNT(dim_risk[risk_key])
```

---
## 7. Visual Recommendations (Updated)
Page 1 – Overview
- KPI Cards: Total Claim Amount, Total Claims, High Severity %, Loss Ratio, Claim Frequency, Processing Review %, Data Quality OK
- Column: Claim Amount by Severity Band
- Stacked Column: Claim Amount by Risk Category & Processing Flag
- Line: Monthly Total Claim Amount vs YoY %
- Map: Total Claims or Claim Amount by location_id (dim_location)

Page 2 – Risk & Telematics
- Matrix: Risk Category x Severity Band (Claim Count, Avg Severity)
- Scatter: Average Severity vs Average Telematics Speed (bubble = Avg Claim Amount)
- Slicers: Severity Band, Processing Flag, Risk Category

Page 3 – Policy / Underwriting
- Loss Ratio by Driver Age Bucket
- Claim Frequency distribution
- Premium vs Claim Amount scatter (color by Severity Band)

Page 4 – Rules
- Funnel: Total Claims → Valid Policy → Valid Amount
- Bar: Counts by rules outcomes

Page 5 – Data Quality
- Table: constraint_audit (filtered for duplicates/orphans)
- Cards: Orphan Claim FK Count, Orphan Location FK Count, Duplicate dim_claim PK Rows

---
## 8. Performance & Optimization
- Use star relationships; avoid bi-directional filters.
- Hide raw key columns on facts after relationships (keep claim_no for drillthrough if needed).
- Prefer `fact_telematics` over wide table for telematics-specific visuals.
- Use `v_claims_summary_by_risk` for high-level dashboards if large claim volumes grow.
- Keep calculation logic in measures; avoid redundant calculated columns.

---
## 9. Security (Optional RLS)
Add future `dim_geography` or `dim_customer` for territory-based RLS. Apply RLS only on dimension tables – not facts.

---
## 10. Validation Checklist (Revised)
| Check | Target |
|-------|--------|
| duplicate_pk_rows for every dim = 0 | constraint_audit |
| All orphan_fk_* metrics = 0 | constraint_audit |
| COUNTROWS(dim_claim) = DISTINCTCOUNT(dim_claim[claim_no]) | Model |
| COUNTROWS(dim_risk) = DISTINCTCOUNT(dim_risk[risk_key]) | Model |
| Total Claims matches SQL DISTINCT count | gold.fact_claims |
| Loss Ratio = SUM(claim_amount_total)/SUM(dim_policy[premium]) | Spot SQL |
| Distinct Locations logical | DISTINCTCOUNT(location_id) stable |
| Data Quality OK = "OK" | After refresh |

---
## 11. Deployment Workflow
1. Load dimension tables, then facts, then governance tables.
2. Hide governance & wide fact (optional) from report view.
3. Create optional Risk Key surrogate if modeling composite relationship.
4. Mark date table, create folders, hide technical columns.
5. Add minimal calculated columns (Severity Band, Age Bucket, Risk Key if used).
6. Implement base measures, then ratios & time series.
7. Build visuals; include Data Quality page early.
8. Validate numbers vs source SQL queries.
9. Publish; configure permissions & (if needed) RLS.
10. Set refresh strategy (Direct Lake typically minimal configuration).

---
## 12. Future Enhancements
- Extend risk attributes to rules fact & add risk_key there if needed.
- SCD Type 2 dimensions (policy, claim status evolution).
- Aggregation tables for month/risk summarization.
- ML-driven propensity / fraud scoring dimension.
- Calculation groups for time & risk scenario switching.

---
**Outcome:** A governed, auditable, high-performance semantic model leveraging natural keys, deterministic GUID locations, optional surrogate risk key, and automated integrity telemetry enabling scalable insurance analytics.
