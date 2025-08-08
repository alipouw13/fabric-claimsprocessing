# Smart Claims Power BI Dashboard Guide

This guide explains how to recreate the Databricks dashboards (Smart Claims Summary Report & Smart Claims Investigation) in Microsoft Fabric Power BI. It includes data model design, DAX measures, visuals, and layout recommendations.

## 1. Data Sources

Use Lakehouse tables (DirectLake or Import):
- `gold_insights` (fact claims & enriched metrics)
- `silver_accident` (image-level predictions, severity, metadata)
- `silver_claim_policy_telematics` (lat/long telemetry path)
- `silver_policy` (policy attributes & effective dates)
- Optional: `silver_claim` (raw claim amounts if separated)
- Optional: Date dimension (recommended)

## 2. Data Model & Relationships

Recommended relationships (single direction):
- `gold_insights[claim_no]` 1:* `silver_accident[claim_no]`
- `gold_insights[claim_no]` 1:* `silver_claim_policy_telematics[claim_no]`
- `gold_insights[policy_no]` 1:* `silver_policy[policy_no]` (if needed)
- `Date[Date]` 1:* `gold_insights[claim_date]`

Mark the `Date` table as a Date table. Create it with DAX:
```DAX
Date = CALENDAR(DATE(2016,1,1), DATE(2025,12,31))
```
Add supporting columns if needed (Year, Month, MonthName, Quarter, etc.).

## 3. Calculated Columns (Flags & Buckets)

In `gold_insights` (use as few columns as possible, prefer measures if dynamic):
```DAX
ValidPolicyFlag = IF(gold_insights[valid_date] = "NOT VALID", 0, 1)
SeverityMatchFlag = IF(gold_insights[reported_severity_check] = "Severity matches the report", 1, 0)
ApproveAmountFlag = IF(gold_insights[valid_amount] = "claim value more than premium", 0, 1)
HighSpeedFlag = 
VAR s = gold_insights[speed_check]
RETURN IF(s = "High Speed", 1, IF(s = "Normal Speed", 0, BLANK()))

SeverityBucket = 
SWITCH(TRUE(),
  gold_insights[severity] >= 0.8, "High",
  gold_insights[severity] >= 0.6, "Medium-High",
  gold_insights[severity] >= 0.4, "Medium",
  gold_insights[severity] >= 0.2, "Low-Medium",
  "Low"
)
```

For images (if URL build required):
```DAX
ImageURL = "https://<storage-path>/" & silver_accident[image_name]
```
Set Data Category = Image URL.

## 4. Core Measures (DAX)

```DAX
Claims = DISTINCTCOUNT(gold_insights[claim_no])
Total Claim Amount = SUM(gold_insights[claim_amount_total])
Injury Amount = SUM(gold_insights[claim_amount_injury])
Property Amount = SUM(gold_insights[claim_amount_property]) + SUM(gold_insights[claim_amount_vehicle])
Valid Policy % = AVERAGE(gold_insights[ValidPolicyFlag])
Severity Match % = AVERAGE(gold_insights[SeverityMatchFlag])
Approve Amount % = AVERAGE(gold_insights[ApproveAmountFlag])
Suspicious Count = CALCULATE(COUNTROWS(gold_insights), gold_insights[suspicious_activity] = TRUE())
Liability Damage = SUM(gold_insights[claim_amount_injury])
Property Damage = SUM(gold_insights[claim_amount_property]) + SUM(gold_insights[claim_amount_vehicle])
Severity Distribution = [Claims]  // Use with SeverityBucket on axis
Avg Speeding Ratio = AVERAGE(gold_insights[HighSpeedFlag])

// Optional Comparison
Severity Agreement Rate = AVERAGE(gold_insights[SeverityMatchFlag])
```

Loss Ratio (if premiums & claims separated):
```DAX
Loss Ratio =
DIVIDE(
  SUM(gold_insights[claim_amount_total]),
  CALCULATE(SUM(silver_policy[premium]))
)
```
(Adjust to mirror complex SQL if period-over-period needed.)

## 5. Report Pages

### Page 1: Claims Summary
Visuals:
- KPI Cards: Claims, Total Claim Amount, Valid Policy %, Severity Match %, Approve Amount %, Suspicious Count
- Column Chart: Incidents by Vehicle Year (Axis: model_year; Value: Claims or count)
- Pie/Donut or 100% Stacked Column: Liability vs Property Damage (Liability Damage, Property Damage)
- Line Chart: Incidents by Hour (incident_hour vs Claims + dual lines for Liability Damage & Property Damage)
- Column / Histogram: Driver Age Distribution (driver_age vs count)
- Stacked Column: Incident Type vs Incident Severity (incident_type axis, incident_severity legend, value count)
- Severity Distribution: Column chart (SeverityBucket vs Claims)
- Map: Policy / Claim Locations (latitude, longitude from `silver_claim_policy_location` if available)

Slicers:
- Date range (Date[Date])
- Incident Type
- SeverityBucket
- Policy No
- Claim No (maybe moved to Investigation page only)

### Page 2: Claims Investigation
Focus on a selected claim:
- Slicers: Claim No, Policy No
- Claim Detail Table: claim_date, incident_type, reported vs predicted severity, reported_severity_check, valid_amount, valid_date, claim_amount_* fields
- Image Gallery: Table visual with `ImageURL`
- Map: Telematics path (Latitude/Longitude with claim filter)
- KPI Indicators: Valid Policy %, Approve Amount %, Severity Match %, Suspicious Count (filtered context)
- Speeding Gauge / Card: Avg Speeding Ratio (filtered by claim)
- Reported vs Predicted Severity: Clustered bar (two measures or raw columns if categorical)

### Page 3: Policy Analytics (Optional)
- Claims per Policy distribution
- Expired Policies Count (measure using pol_expiry_date < claim_date)
- Loss Ratio trend (by month/quarter)
- Average Claim Amount per Policy

## 6. Drillthrough
Create a Drillthrough page (Claim Detail):
- Add Drillthrough field: claim_no
- Provide full metrics and images
Right-click any claim in summary visuals → Drillthrough.

## 7. Performance Tips
- Use DirectLake for large tables when possible.
- Avoid high-cardinality visuals (limit table row count or enable paging).
- Disable unnecessary interactions (Format → Edit interactions) to reduce query chatter.
- Pre-create measures instead of repeated implicit aggregations.
- Limit calculated columns; prefer measures when dependent on filters.

## 8. Security (Optional)
Row Level Security examples:
```DAX
-- Role: PolicyRegionFilter
[policy_region] = USERPRINCIPALNAME()  // or map via bridge table
```
Publish roles and assign in workspace.

## 9. Validation Checklist
- DISTINCTCOUNT(claim_no) matches SQL baseline.
- Sample claim details match underlying `gold_insights` rows.
- Map points count = telematics records for selected claim.
- Image gallery loads all images per claim.
- Loss ratio measure aligns with SQL audit query output.

## 10. Deployment Steps
1. Get Data → OneLake / Lakehouse tables.
2. Create relationships & mark Date table.
3. Add calculated columns & measures.
4. Build visuals per page layout.
5. Configure slicers, drillthrough, bookmarks (if needed).
6. Apply theme / corporate colors.
7. Validate metrics vs SQL queries (export sample).
8. Publish to Fabric workspace; set refresh or rely on DirectLake.
9. Share report & optionally pin high-level KPIs to a dashboard.

## 11. Future Enhancements
- Add incremental refresh for large historical claim tables.
- Implement calculation groups for dynamic measure switching (Damage Type selector).
- Add anomaly detection visual using AI (Fabric advanced visuals).
- Integrate AI Q&A (enable Q&A visual on curated fields).

---
**Outcome:** This blueprint replicates and extends the Databricks dashboards in Power BI with maintainable DAX, scalable model design, and investigative workflows.
