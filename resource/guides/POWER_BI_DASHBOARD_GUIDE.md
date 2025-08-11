# Smart Claims Power BI Dashboard Guide (Updated Star / Snowflake Schema – Natural Key & Audit Enhanced)

This guide reflects the latest gold layer design where dimension and fact entities are **physical Delta tables** (not only views) with enforced logical primary keys (via deduping) plus a `gold.constraint_audit` table for data quality. It supersedes earlier guidance that relied purely on materialized views with potential duplicate natural keys.

---
## 1. Data Sources (Gold Layer Tables & Views)
Primary gold Delta tables (Direct Lake recommended):
### Dimensions (PKs shown)
- `gold.dim_date` (PK: date_key)
- `gold.dim_claim` (PK: claim_no – 1 row per claim, latest snapshot)
- `gold.dim_policy` (PK: policy_no)
- `gold.dim_vehicle` (PK: chassis_no)
- `gold.dim_location` (Composite natural key: (zip_code, latitude, longitude))
- `gold.dim_risk` (Composite natural key: (severity_category, risk_category, processing_flag, speed_risk_indicator))

### Facts
- `gold.fact_claims` (grain: 1 row per claim; central fact for most KPIs)
- `gold.fact_telematics` (grain: claim_no + chassis_no; telematics summary at claim context)
- `gold.fact_accident_severity` (grain: claim snapshot – may retain historical if extended later)
- `gold.fact_rules` (optional; exists if rules engine produced `gold.gold_insights`)
- `gold.fact_smart_claims` (wide integrated table – prototyping only; hide in final model)

### Reporting / Aggregation Views
- `gold.v_claims_summary_by_risk`
- `gold.v_smart_claims_dashboard`

### Data Quality / Governance
- `gold.constraint_audit` (rows of metrics: entity | metric | value) – validates PK uniqueness & orphan FK counts.

> NOTE: Dimensions & facts were rebuilt to eliminate duplicates via ROW_NUMBER() (dim_claim) or DISTINCT logic (others). There are no enforced physical constraints in Delta; logical enforcement occurs in build & audit.

---
## 2. Star vs Snowflake Schema Strategy
Current delivery is a **clean star** centered on `fact_claims` with some *degenerate / repeating attributes* still present in facts (e.g., severity_category) for convenience. A *micro-snowflake* can optionally emerge if you choose to:
- Create a surrogate single-column key for `dim_risk` (because Power BI cannot form a multi-column relationship). 
- Create a surrogate key for `dim_location` if geospatial slicing demands a single key.

### Recommended Approach
1. Keep `fact_claims` as central fact; use natural keys for relationships where single-column (claim_no, policy_no, chassis_no, date_key).
2. For `dim_risk` and `dim_location`, create surrogate keys in Power Query (M) or as calculated columns:
   - Risk Key (calculated column in dim_risk):
     ```DAX
     dim_risk[Risk Key] = 
       dim_risk[severity_category] & "|" & dim_risk[risk_category] & "|" & dim_risk[processing_flag] & "|" & dim_risk[speed_risk_indicator]
     ```
   - Location Key (calculated column in dim_location):
     ```DAX
     dim_location[Location Key] = 
       COALESCE(dim_location[zip_code], "NA") & "|" & 
       COALESCE(FORMAT(dim_location[latitude], "0.#####"), "NA") & "|" & 
       COALESCE(FORMAT(dim_location[longitude], "0.#####"), "NA")
     ```
   - Mirror keys in `fact_claims` via calculated columns OR (preferred) add them during the gold build later for performance.

### Relationship Matrix (Natural / Surrogate)
| From Dim        | To Fact          | Key Strategy          | Cardinality |
|-----------------|------------------|-----------------------|-------------|
| dim_claim       | fact_claims      | claim_no              | 1:*         |
| dim_policy      | fact_claims      | policy_no             | 1:*         |
| dim_vehicle     | fact_claims      | chassis_no            | 1:*         |
| dim_date        | fact_claims      | date_key → claim_date | 1:* (mark dim_date) |
| dim_risk        | fact_claims      | Risk Key (surrogate)  | 1:* (after key added) |
| dim_location    | fact_claims      | Location Key (surrogate) | 1:* (optional) |
| dim_claim       | fact_rules       | claim_no              | 1:*         |
| dim_claim       | fact_telematics  | claim_no              | 1:*         |

If you do not introduce surrogate keys, treat risk/location attributes directly on the fact (denormalized usage). This is acceptable for initial delivery.

---
## 3. Primary Key (PK) & Foreign Key (FK) Logic
Logical PK validation occurs in build script; duplicates eliminated. Use `gold.constraint_audit` to verify:
- `duplicate_pk_rows` = 0 for each dimension
- `orphan_fk_<col>_to_<dim>` = 0 for each fact/dimension pairing

Add a Power BI data quality page sourcing `constraint_audit` to monitor model health.

Sample audit table rows (conceptually):
| entity | metric | value |
|--------|--------|-------|
| dim_claim | duplicate_pk_rows | 0 |
| fact_claims | orphan_fk_claim_no_to_dim_claim | 0 |

---
## 4. Model Loading Order
1. Load dimensions (date, claim, policy, vehicle, risk, location). 
2. Load facts (claims first, then telematics, accident severity, rules, wide fact if needed).
3. Load `constraint_audit` last (mark as hidden except in quality report).
4. Create surrogate keys (if used) before establishing relationships.
5. Mark `dim_date[date_key]` as the date table.

---
## 5. Calculated Columns (Minimal Set)
Only add if surrogate keys or semantic groupings required:
```DAX
-- In dim_claim (Severity Band) – unchanged logic but now dimension is unique per claim
Severity Band = 
SWITCH(TRUE(),
  dim_claim[severity] >= 0.8, "High",
  dim_claim[severity] >= 0.6, "Medium-High",
  dim_claim[severity] >= 0.4, "Medium",
  dim_claim[severity] >= 0.2, "Low-Medium",
  "Low"
)

-- In dim_policy (Driver Age Bucket)
Driver Age Bucket = 
SWITCH(TRUE(),
  dim_policy[driver_age] < 25, "<25",
  dim_policy[driver_age] < 35, "25-34",
  dim_policy[driver_age] < 50, "35-49",
  dim_policy[driver_age] < 65, "50-64",
  "65+"
)
```
Optional surrogate keys (if implementing snowflake style): see Section 2.

---
## 6. Measure Design Principles
- Separate base aggregations from derivative ratios.
- Use natural naming, group by folders: Volume, Financial, Severity, Risk, Telematics, Rules, Time Intelligence, Quality.
- Use `DIVIDE()` over `/`.
- Add data quality measures referencing `constraint_audit`.

### 6.1 Base Measures (Fact: fact_claims)
```DAX
-- Volume
Total Claims = DISTINCTCOUNT(fact_claims[claim_no])
Total Policies = DISTINCTCOUNT(fact_claims[policy_no])
Total Vehicles = DISTINCTCOUNT(fact_claims[chassis_no])

-- Financial
Total Claim Amount = SUM(fact_claims[claim_amount_total])
Vehicle Claim Amount = SUM(fact_claims[claim_amount_vehicle])
Injury Claim Amount = SUM(fact_claims[claim_amount_injury])
Property Claim Amount = SUM(fact_claims[claim_amount_property])
Avg Claim Amount = DIVIDE([Total Claim Amount], [Total Claims])
Exposure (Sum Insured) = SUM(fact_claims[sum_insured])

-- Severity & Risk
Average Severity = AVERAGE(fact_claims[severity])
High Severity Claims = CALCULATE([Total Claims], fact_claims[severity_category] IN {"High","Medium-High"})
High Severity % = DIVIDE([High Severity Claims],[Total Claims])
Risk Weighted Amount = SUMX(fact_claims, fact_claims[claim_amount_total] * fact_claims[severity])

-- Processing Flags
Processing Review Claims = CALCULATE([Total Claims], fact_claims[processing_flag] = "REVIEW_REQUIRED")
Processing Review % = DIVIDE([Processing Review Claims],[Total Claims])
Total Loss Candidates = CALCULATE([Total Claims], fact_claims[processing_flag] = "TOTAL_LOSS_CANDIDATE")

-- Telematics (if present in fact_claims)
Average Telematics Speed = AVERAGE(fact_smart_claims[telematics_speed])
High Speed Risk Claims = CALCULATE([Total Claims], fact_smart_claims[speed_risk_indicator] IN {"HIGH_SPEED_SEVERE","MODERATE_SPEED_RISK"})
High Speed Risk % = DIVIDE([High Speed Risk Claims],[Total Claims])

-- Rules (bridges via claim_no; requires fact_rules loaded)
Valid Policy Claims = CALCULATE([Total Claims], fact_rules[valid_date] = "VALID")
Valid Policy % = DIVIDE([Valid Policy Claims],[Total Claims])
Valid Amount Claims = CALCULATE([Total Claims], fact_rules[valid_amount] = "claim value in the range of premium")
Valid Amount % = DIVIDE([Valid Amount Claims],[Total Claims])

-- Premium (policy dimension)
Premium Amount = SUM(dim_policy[premium])
Loss Ratio = DIVIDE([Total Claim Amount], [Premium Amount])

-- Frequency & Derived
Claim Frequency = DIVIDE([Total Claims],[Total Policies])
Avg Accident-Event Distance (Miles) = AVERAGE(fact_claims[accident_telematics_distance_miles])
```

### 6.2 Time Intelligence (Requires dim_date marked)
```DAX
Total Claim Amount PY = CALCULATE([Total Claim Amount], DATEADD(dim_date[date_key], -1, YEAR))
YoY Claim Amount % = 
VAR Curr = [Total Claim Amount]
VAR Prev = [Total Claim Amount PY]
RETURN DIVIDE(Curr - Prev, Prev)
```

### 6.3 Data Quality (Constraint Audit)
Bring `gold.constraint_audit` in as a table named `constraint_audit` (hide by default):
```DAX
Orphan Claim FK Count = 
VAR v = CALCULATE( MAX ( constraint_audit[value] ), 
                  constraint_audit[entity] = "fact_claims", 
                  constraint_audit[metric] = "orphan_fk_claim_no_to_dim_claim" )
RETURN VALUE(v)

Duplicate dim_claim PK Rows = 
VAR v = CALCULATE( MAX ( constraint_audit[value] ), 
                  constraint_audit[entity] = "dim_claim", 
                  constraint_audit[metric] = "duplicate_pk_rows" )
RETURN VALUE(v)

Data Quality OK = 
IF( [Orphan Claim FK Count] = 0 && [Duplicate dim_claim PK Rows] = 0, "OK", "ISSUES" )
```
Create a conditional formatting rule for a status card using `Data Quality OK`.

### 6.4 Risk Dimension (If Surrogate Key Implemented)
```DAX
Distinct Risk Profiles = DISTINCTCOUNT(dim_risk[Risk Key])
```

---
## 7. Visual Layer Recommendations (Updated)
Page 1 – Executive Overview:
- KPIs: Total Claim Amount, Total Claims, High Severity %, Loss Ratio, Claim Frequency, Processing Review %.
- Column: Claim Amount by Severity Band.
- Stacked Column: Total Claim Amount by Risk Category & Processing Flag.
- Line: Total Claim Amount vs YoY Claim Amount % (secondary axis optional).
- Map: Claims by Location (using dim_location or ZIP).
- Card: Data Quality OK.

Page 2 – Risk & Behavior:
- Matrix: Risk Category x Severity Band (Claim Count, Avg Claim Amount, Average Severity).
- Scatter: Average Severity vs Average Telematics Speed (bubble = Avg Claim Amount).
- Slicer: Risk Key (if implemented) or separate risk dimension attributes.

Page 3 – Underwriting & Policy:
- Histogram: Claim Frequency distribution.
- Bar: Loss Ratio by Driver Age Bucket.
- Scatter: Premium Amount vs Total Claim Amount (color by Severity Band).

Page 4 – Rules / Workflow:
- Funnel: Claims → Valid Policy → Valid Amount → Release Funds.
- Bar: Counts by Rule Outcome (release_funds, valid_date, valid_amount).

Page 5 – Data Quality:
- Table from constraint_audit (entity, metric, value).
- Cards: Orphan Claim FK Count, Duplicate dim_claim PK Rows.
- Status: Data Quality OK.

---
## 8. Performance & Optimization
- Favor relationships over repeated filters; keep star shape.
- Avoid bi-directional filters; use DAX when needed.
- Hide unused natural key columns on facts after relationships.
- Consider incremental refresh only if you move from Direct Lake to Import / Hybrid.
- Pre-aggregate heavy visuals via `v_claims_summary_by_risk`.

---
## 9. Security (Optional RLS)
If region or customer segmentation added later, create a dedicated dimension (e.g., dim_geography) and apply RLS there. Keep rules logic outside facts for maintainability.

---
## 10. Validation Checklist (Updated)
| Check | Method |
|-------|--------|
| Distinct claim_no in dim_claim equals row count | COUNTROWS(dim_claim) = DISTINCTCOUNT(dim_claim[claim_no]) |
| No duplicate PK in constraint_audit | duplicate_pk_rows = 0 |
| Orphan FKs zero | All orphan_fk_* metrics = 0 |
| Total Claims measure matches SQL | Compare to COUNT(DISTINCT claim_no) in gold.fact_claims |
| Loss Ratio computation | SUM(claim_amount_total)/SUM(dim_policy[premium]) |
| Surrogate Risk Key consistent | DISTINCTCOUNT(Risk Key) stable across refresh |

---
## 11. Deployment Workflow (Revised)
1. Connect to Lakehouse; select `gold` tables first (dims → facts) then views.
2. Load `constraint_audit` (hide table; expose measures).
3. Create surrogate keys (if chosen) & relationships.
4. Mark date table; configure folders and hide technical columns.
5. Add calculated columns (Severity Band, Age Bucket) sparingly.
6. Implement base measures, then ratios & time intelligence.
7. Build visuals per page plan; add Data Quality page early.
8. Test refresh & cross-filter behavior; validate metrics vs SQL.
9. Publish; enable sensitivity labels if required.
10. Iterate toward surrogate keys removal from facts if fully normalized.

---
## 12. Future Enhancements
- Add slowly changing dimension (SCD) handling for policy or claim status snapshots.
- Introduce dedicated geography dimension & hierarchical drill (Country > Region > ZIP).
- Calculation Groups for Time Intelligence & Risk Scenarios.
- Add ML scoring dimension for advanced risk clusters.
- Parameter-driven dynamic segmentation thresholds (What-If parameters).

---
**Outcome:** A governed, auditable semantic model leveraging physically deduped gold tables with natural keys, optional surrogate key snowflake extensions, automated data quality telemetry, and optimized DAX structure for scalable insurance analytics.
