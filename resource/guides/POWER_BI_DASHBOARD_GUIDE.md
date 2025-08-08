# Smart Claims Power BI Dashboard Guide (Updated Star Schema Version)

This guide explains how to build a scalable, maintainable Power BI semantic model and dashboards over the Fabric Lakehouse Gold layer. It replaces prior direct usage of mixed Silver / gold_insights tables with a curated star schema of materialized views in the `gold` schema.

---
## 1. Data Sources (Gold Layer)
Primary gold materialized views (Direct Lake recommended):
- Dimensions:
  - `gold.dim_date`
  - `gold.dim_claim`
  - `gold.dim_policy`
  - `gold.dim_vehicle`
  - `gold.dim_location`
  - `gold.dim_risk`
- Facts:
  - `gold.fact_claims` (central grain: 1 row per claim)
  - `gold.fact_rules` (rule evaluation results, claim grain – may left join or treat as fact-outrigger)
  - `gold.fact_telematics` (claim / chassis-level telematics summary attributes; may be separate fact or merged)
  - `gold.fact_accident_severity` (image / claim severity history – optional long-term analysis)
  - `gold.fact_smart_claims` (wide integrated table – use only for rapid prototyping, then hide)
- Aggregations:
  - `gold.v_claims_summary_by_risk` (pre-aggregated – can accelerate visuals at risk category grain)
  - `gold.v_smart_claims_dashboard` (flattened for quick ad‑hoc exploration)

Recommended Core Model (lean): dim_* + fact_claims (+ optionally fact_rules). Add other facts later as needed.

---
## 2. Star Schema & Relationships
Load dimensions first, then facts. Define single-direction (dim ➜ fact) relationships:
| From (Dim) | Column | To (Fact) | Column | Cardinality |
|------------|--------|-----------|--------|-------------|
| dim_claim  | claim_no | fact_claims | claim_no | 1:* |
| dim_policy | policy_no | fact_claims | policy_no | 1:* |
| dim_vehicle| chassis_no | fact_claims | chassis_no | 1:* |
| dim_risk   | severity_category | fact_claims | severity_category | *:* (make supporting; optionally build surrogate key) |
| dim_date   | date_key | fact_claims | claim_date | 1:* |
| dim_location | ZIP_CODE | fact_claims | ZIP_CODE | *:* (optional; many claims per ZIP – acceptable) |
| dim_claim  | claim_no | fact_rules | claim_no | 1:* (if fact_rules loaded) |

Best Practices:
- Mark `dim_date` as Date table.
- Hide key columns on fact tables (claim_no, policy_no, etc.) after relationships established (leave one visible key for drillthrough if desired).
- Consider a surrogate risk key if combining multiple risk attributes; otherwise treat each attribute as its own slicer.
- Create role-playing date dimensions (e.g., Policy Effective Date) by duplicating dim_date via calculated table if needed.

Optional Simplification Path:
- For initial build, use only `fact_claims` + `dim_date` + `dim_policy` + `dim_vehicle` + `dim_risk`.
- Later add `fact_rules` for workflow KPIs, and `fact_telematics` for behavioral analytics.

---
## 3. Column Usage Guidelines
Prefer measures over calculated columns. Only create calculated columns when:
- Relationship / key normalization required (e.g., derived bucket not easily expressed as dynamic measure context).
- Static attribute needed for slicer that does not change with filters.

Suggested Minimal Calculated Columns:
In `dim_claim`:
```DAX
dim_claim[Severity Band] =
SWITCH(TRUE(),
  dim_claim[severity] >= 0.8, "High",
  dim_claim[severity] >= 0.6, "Medium-High",
  dim_claim[severity] >= 0.4, "Medium",
  dim_claim[severity] >= 0.2, "Low-Medium",
  "Low"
)
```
In `dim_claim` (Age Buckets if driver_age existed there; otherwise in policy dim):
```DAX
dim_policy[Driver Age Bucket] =
SWITCH(TRUE(),
  dim_policy[driver_age] < 25, "<25",
  dim_policy[driver_age] < 35, "25-34",
  dim_policy[driver_age] < 50, "35-49",
  dim_policy[driver_age] < 65, "50-64",
  "65+"
)
```
Avoid duplicating severity band logic across tables.

---
## 4. Measure Design Principles
Naming Conventions:
- Core aggregations: Pascal Case (e.g., `Total Claims`)
- Ratios / percentages end with `%`
- Flags prefixed with descriptor (e.g., `High Severity Claims`)
- Avoid embedding filter logic in multiple measures; build base + derivative.

Foldering (Display Folders):
- Volume, Financial, Severity, Risk, Quality, Telematics, Rules, Time Intelligence.

Re-use patterns (`VAR` for clarity, `DIVIDE` for safety), and isolate reused sets in base measures.

---
## 5. Core DAX Measures (Fact: fact_claims)
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

-- Severity / Risk
Average Severity = AVERAGE(fact_claims[severity])
High Severity Claims = CALCULATE([Total Claims], fact_claims[severity_category] IN {"High","Medium-High"})
High Severity % = DIVIDE([High Severity Claims], [Total Claims])
Risk Weighted Amount = SUMX(fact_claims, fact_claims[claim_amount_total] * fact_claims[severity])

-- Processing / Workflow
Processing Review Claims = CALCULATE([Total Claims], fact_claims[processing_flag] = "REVIEW_REQUIRED")
Processing Review % = DIVIDE([Processing Review Claims],[Total Claims])
Total Loss Candidates = CALCULATE([Total Claims], fact_claims[processing_flag] = "TOTAL_LOSS_CANDIDATE")

-- Speed / Behavior (if telematics columns present in fact_claims; else use fact_telematics)
Average Telematics Speed = AVERAGE(fact_claims[telematics_speed])
High Speed Risk Claims = CALCULATE([Total Claims], fact_claims[speed_risk_indicator] IN {"HIGH_SPEED_SEVERE","MODERATE_SPEED_RISK"})
High Speed Risk % = DIVIDE([High Speed Risk Claims],[Total Claims])

-- Rules (joins fact_rules via claim_no)
Release Funds Claims = CALCULATE([Total Claims], fact_rules[release_funds] = "release funds")
Release Funds % = DIVIDE([Release Funds Claims],[Total Claims])
Valid Policy Claims = CALCULATE([Total Claims], fact_rules[valid_date] = "VALID")
Valid Policy % = DIVIDE([Valid Policy Claims],[Total Claims])
Valid Amount Claims = CALCULATE([Total Claims], fact_rules[valid_amount] = "claim value in the range of premium")
Valid Amount % = DIVIDE([Valid Amount Claims],[Total Claims])
Severity Agreement Claims = CALCULATE([Total Claims], fact_rules[reported_severity_check] = "Severity matches the report")
Severity Agreement % = DIVIDE([Severity Agreement Claims],[Total Claims])

-- Loss Ratio (simplified; requires premium by policy – if premium is in dim_policy link via relationship)
Premium Amount = SUM(dim_policy[premium])
Loss Ratio = DIVIDE([Total Claim Amount], [Premium Amount])

-- Claim Frequency (claims per policy)
Claim Frequency = DIVIDE([Total Claims], [Total Policies])

-- Distribution Helpers (used in visuals)
Severity Band Claims = [Total Claims]  -- Place Severity Band (dim_claim[Severity Band]) on axis
Risk Category Claims = [Total Claims]  -- Place risk_category from dim_claim or dim_risk

-- Time Intelligence (example: requires dim_date marked date table)
Total Claim Amount PY = CALCULATE([Total Claim Amount], DATEADD(dim_date[date_key], -1, YEAR))
YoY Claim Amount % = DIVIDE([Total Claim Amount] - [Total Claim Amount PY], [Total Claim Amount PY])

-- Data Quality / Distance
Avg Accident-Event Distance (Miles) = AVERAGE(fact_claims[accident_telematics_distance_miles])
```

Optional Percentile Measures (approximate pattern):
```DAX
-- Requires a separate calculation table or summarized values; placeholder for advanced stats.
```

---
## 6. Additional Fact / Telematics Measures (If using fact_telematics)
```DAX
Telematics Records = COUNTROWS(fact_telematics)
Avg Speed (Telematics Fact) = AVERAGE(fact_telematics[telematics_speed])
Speed StdDev (Telematics) = AVERAGE(fact_telematics[telematics_speed_std])
Speed Severity Interaction = AVERAGEX(fact_telematics, fact_telematics[telematics_speed] * RELATED(fact_claims[severity]))
```

---
## 7. Visual Layer Recommendations
Page 1 – Executive Overview:
- KPI Cards: Total Claim Amount, Total Claims, High Severity %, Release Funds %, Loss Ratio, Claim Frequency.
- Bar: Claim Amount by Severity Band.
- Stacked Bar: Claim Amount by Risk Category & Processing Flag.
- Line: Total Claim Amount (Month) with YoY comparator.
- Scatter: Severity vs Telematics Speed (color by Risk Category).
- Map: Claims by ZIP_CODE (size: Total Claim Amount, color: High Severity %).
- Matrix: Risk Category vs Severity Band (claim_count, avg_claim_amount).

Page 2 – Investigation:
- Slicer Panel: Claim No, Policy No, Severity Band, Risk Category.
- Claim Detail Table: severity, severity_category, processing_flag, release_funds, claim_amount_*.
- KPI Row: Valid Policy %, Valid Amount %, Severity Agreement %, Release Funds %.
- Speed Trend / Distribution (if time-grain data present).
- Drillthrough link to Image / Telemetry detail (if separate report or custom visual).

Page 3 – Policy / Underwriting:
- Claims per Policy Histogram (Claim Frequency).
- Loss Ratio by Driver Age Bucket.
- Premium vs Claim Amount scatter (bubble size = severity average).

Page 4 – Operations / Rules:
- Rule Effectiveness: Count of claims failing each primary rule (stacked bar).
- Funnel: Total Claims → Valid Policy Claims → Valid Amount Claims → Severity Agreement → Release Funds.

---
## 8. Drillthrough & Navigation
- Create Drillthrough on `claim_no` with detailed rule outcomes & severity history.
- Bookmark set for toggling between aggregated and detailed telematics visuals.

---
## 9. Performance Tips
- Use star schema – avoid wide table joins at visual time.
- Hide unused columns & disable auto date/time.
- Avoid bi-directional relationships; use explicit DAX if cross-filter needed.
- Use pre-aggregated view `gold.v_claims_summary_by_risk` for high-level cards (Direct Lake can fold).
- Group measures into display folders for discoverability.

---
## 10. Security (Optional)
Row-Level Security Example (Region-based – requires region attribute in a dim):
```DAX
[UserRegion] = LOOKUPVALUE(dim_policy[territory], dim_policy[policy_no], SELECTEDVALUE(fact_claims[policy_no])) = USERPRINCIPALNAME()
```
Leverage Azure AD groups for assignment.

---
## 11. Validation Checklist
- `Total Claims` matches SQL COUNT DISTINCT claim_no in `gold.fact_claims`.
- Loss Ratio equals manual division of sums (spot check sample periods).
- High Severity % equals (# severity_category in {High, Medium-High}) ÷ Total Claims.
- Release Funds % matches query on `gold.fact_rules` where release_funds = 'release funds'.
- YoY checks reconcile using exported month-level aggregates.

---
## 12. Deployment Workflow
1. Connect to Lakehouse -> Select `gold` schema views.
2. Load dims, then facts; set relationships.
3. Mark `dim_date` as Date table.
4. Create calculated columns (severity band, age bucket) sparingly.
5. Add core measures from this guide.
6. Build visuals per page design.
7. Add drillthrough pages & bookmarks.
8. Test RLS (if configured).
9. Publish & set refresh (Direct Lake generally no schedule needed unless mixing Import).
10. Document measures (description property) for maintainability.

---
## 13. Future Enhancements
- Calculation Groups (Time Intelligence, Scenario Switching).
- Incremental historical retention & snapshot fact for reserve tracking.
- Advanced anomaly scoring (add ML outputs to dim_risk).
- KPI thresholds table (parameter-driven conditional formatting).
- Decomposition Tree & AI Insights for claim drivers.

---
**Outcome:** A clean star-schema semantic layer enabling performant Direct Lake dashboards with transparent, reusable DAX measures and extensibility for advanced insurance analytics.
