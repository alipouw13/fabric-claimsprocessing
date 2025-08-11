# Microsoft Fabric notebook source
# NOTE: Attach your lakehouse to this notebook to ensure no errors
# Purpose: Build silver integration & gold analytic views (Python version of original SQL script)

from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

print("✅ Starting Gold views build (Python version)")

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
SILVER = "silver"
GOLD = "gold"

base_silver_tables = [
    f"{SILVER}.silver_claim_policy_location",
    f"{SILVER}.silver_telematics",
    f"{SILVER}.silver_accident"
]

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def table_exists(table_name: str) -> bool:
    try:
        spark.table(table_name).limit(1)
        return True
    except Exception:
        return False

def require_tables(tables):
    missing = [t for t in tables if not table_exists(t)]
    if missing:
        raise RuntimeError(f"Missing required silver tables: {', '.join(missing)}. Run upstream notebooks first.")
    print(f"✅ Found all required silver tables: {', '.join(tables)}")

# -----------------------------------------------------------------------------
# Ensure schemas
# -----------------------------------------------------------------------------
for schema in (SILVER, GOLD):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
print("✅ Schemas ensured")

# -----------------------------------------------------------------------------
# Validate source tables
# -----------------------------------------------------------------------------
require_tables(base_silver_tables)

print("📊 Source table record counts:")
for t in base_silver_tables:
    cnt = spark.table(t).count()
    print(f"   {t}: {cnt:,} rows")

# -----------------------------------------------------------------------------
# Create silver aggregated / integrated tables
# -----------------------------------------------------------------------------
print("🛠 Building silver aggregated telematics table (avg)...")
# Averaged telematics per chassis
spark.sql(f"""
CREATE OR REPLACE TABLE {SILVER}.silver_claim_policy_telematics_avg
USING DELTA AS
SELECT 
    p_c.*, 
    t.telematics_speed,
    t.telematics_latitude,
    t.telematics_longitude,
    t.telematics_speed_std,
    t.telematics_record_count,
    t.telematics_latest_timestamp
FROM {SILVER}.silver_claim_policy_location p_c
INNER JOIN (
    SELECT 
        chassis_no,
        ROUND(AVG(speed), 2) AS telematics_speed,
        ROUND(AVG(latitude), 6) AS telematics_latitude,
        ROUND(AVG(longitude), 6) AS telematics_longitude,
        ROUND(STDDEV(speed), 2) AS telematics_speed_std,
        COUNT(*) AS telematics_record_count,
        MAX(event_timestamp) AS telematics_latest_timestamp
    FROM {SILVER}.silver_telematics
    WHERE chassis_no IS NOT NULL
      AND speed IS NOT NULL
      AND latitude IS NOT NULL
      AND longitude IS NOT NULL
    GROUP BY chassis_no
) t ON p_c.chassis_no = t.chassis_no
""")
print("✅ Created silver_claim_policy_telematics_avg")

print("🛠 Building silver detailed telematics table (all events)...")
spark.sql(f"""
CREATE OR REPLACE TABLE {SILVER}.silver_claim_policy_telematics
USING DELTA AS
SELECT 
    p_c.*, 
    t.latitude  AS telematics_latitude,
    t.longitude AS telematics_longitude,
    t.event_timestamp AS telematics_timestamp,
    t.speed AS telematics_speed,
    EXTRACT(HOUR FROM t.event_timestamp)      AS telematics_hour,
    EXTRACT(DAYOFWEEK FROM t.event_timestamp) AS telematics_day_of_week,
    CASE 
        WHEN t.speed > 200 THEN 'ANOMALY_HIGH_SPEED'
        WHEN t.speed < 0 THEN 'ANOMALY_NEGATIVE_SPEED'
        WHEN t.latitude < -90 OR t.latitude > 90 THEN 'ANOMALY_INVALID_LAT'
        WHEN t.longitude < -180 OR t.longitude > 180 THEN 'ANOMALY_INVALID_LON'
        ELSE 'VALID'
    END AS data_quality_flag
FROM {SILVER}.silver_telematics t
INNER JOIN {SILVER}.silver_claim_policy_location p_c
    ON p_c.chassis_no = t.chassis_no
WHERE t.chassis_no IS NOT NULL
  AND t.event_timestamp IS NOT NULL
""")
print("✅ Created silver_claim_policy_telematics")

print("🛠 Building silver claim-policy-accident integration table...")
spark.sql(f"""
CREATE OR REPLACE TABLE {SILVER}.silver_claim_policy_accident
USING DELTA AS
SELECT 
    p_c.*,
    a.severity,
    a.severity_category,
    a.image_name,
    a.processed_timestamp AS accident_processing_timestamp,
    a.data_quality_score AS accident_data_quality,
    CASE 
        WHEN a.severity >= 0.8 THEN 'HIGH_RISK'
        WHEN a.severity >= 0.5 THEN 'MEDIUM_RISK'
        WHEN a.severity >= 0.2 THEN 'LOW_RISK'
        ELSE 'MINIMAL_RISK'
    END AS risk_category,
    CASE 
        WHEN a.severity > 0.7 AND p_c.premium < 1000 THEN 'REVIEW_REQUIRED'
        WHEN a.severity > 0.9 THEN 'TOTAL_LOSS_CANDIDATE'
        ELSE 'STANDARD_PROCESSING'
    END AS processing_flag
FROM {SILVER}.silver_claim_policy_telematics_avg p_c
LEFT JOIN {SILVER}.silver_accident a
  ON p_c.claim_no = a.claim_no
""")
print("✅ Created silver_claim_policy_accident")

print("🛠 Building comprehensive analytics table (silver_smart_claims_analytics)...")
spark.sql(f"""
CREATE OR REPLACE TABLE {SILVER}.silver_smart_claims_analytics
USING DELTA AS
SELECT 
    pca.claim_no,
    pca.chassis_no,
    pca.policy_no,
    pca.cust_id,
    pca.driver_age,
    pca.premium,
    pca.pol_issue_date,
    pca.pol_expiry_date,
    pca.make,
    pca.model,
    pca.model_year,
    pca.suspicious_activity,
    pca.sum_insured,
    pca.claim_date,
    pca.claim_amount_total,
    pca.claim_amount_vehicle,
    pca.claim_amount_injury,
    pca.claim_amount_property,
    pca.latitude,
    pca.longitude,
    pca.borough,
    pca.neighborhood,
    pca.zip_code,
    pca.incident_severity,
    pca.severity,
    pca.severity_category,
    pca.risk_category,
    pca.processing_flag,
    pca.telematics_latitude,
    pca.telematics_longitude,
    pca.telematics_speed,
    pca.telematics_speed_std,
    CASE 
        WHEN pca.telematics_speed > 80 AND pca.severity > 0.6 THEN 'HIGH_SPEED_SEVERE'
        WHEN pca.telematics_speed > 60 AND pca.severity > 0.4 THEN 'MODERATE_SPEED_RISK'
        ELSE 'NORMAL_RISK'
    END AS speed_risk_indicator,
    SQRT( POW((pca.latitude - pca.telematics_latitude) * 69, 2) +
          POW((pca.longitude - pca.telematics_longitude) * 54.6, 2)) AS accident_telematics_distance_miles,
    CURRENT_TIMESTAMP() AS analytics_created_timestamp,
    'fabric_lakehouse' AS data_source
FROM {SILVER}.silver_claim_policy_accident pca
WHERE pca.claim_no IS NOT NULL
  AND pca.chassis_no IS NOT NULL
""")
print("✅ Created silver_smart_claims_analytics")

# -----------------------------------------------------------------------------
# Basic validation summary
# -----------------------------------------------------------------------------
print("🔍 Validation summary:")
validation_df = spark.sql(f"""
SELECT 'silver_claim_policy_telematics_avg' AS tbl, COUNT(*) AS rows FROM {SILVER}.silver_claim_policy_telematics_avg UNION ALL
SELECT 'silver_claim_policy_telematics'     AS tbl, COUNT(*) AS rows FROM {SILVER}.silver_claim_policy_telematics UNION ALL
SELECT 'silver_claim_policy_accident'       AS tbl, COUNT(*) AS rows FROM {SILVER}.silver_claim_policy_accident UNION ALL
SELECT 'silver_smart_claims_analytics'      AS tbl, COUNT(*) AS rows FROM {SILVER}.silver_smart_claims_analytics
""")
validation_df.show(truncate=False)

# -----------------------------------------------------------------------------
# GOLD dimension & fact TABLES with natural key dedupe + constraint audit
# -----------------------------------------------------------------------------
print("🛠 Building gold dimension & fact tables (natural keys, deduped)...")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD}")

# Utility to drop any existing object (view/materialized/table)

def _drop_object(name: str):
    for kind in ["MATERIALIZED LAKE VIEW", "MATERIALIZED VIEW", "VIEW"]:
        try:
            spark.sql(f"DROP {kind} IF EXISTS {name}")
        except Exception:
            pass
    spark.sql(f"DROP TABLE IF EXISTS {name}")

metrics = []  # (entity, metric, value)

def _record(entity, metric, value):
    metrics.append((entity, metric, int(value) if isinstance(value, (int, bool)) else float(value) if isinstance(value, float) else value))

# Dimension builders (natural PK definitions)

# dim_date (PK: date_key)
_drop_object(f"{GOLD}.dim_date")
_dim_date_sql = f"""
WITH bounds AS (
  SELECT date(min(claim_date)) AS min_d, date(max(claim_date)) AS max_d FROM {SILVER}.silver_smart_claims_analytics
), calendar AS (
  SELECT explode(sequence(min_d, max_d, interval 1 day)) AS date_key FROM bounds
)
SELECT 
  date_key,
  year(date_key)        AS year,
  quarter(date_key)     AS quarter,
  month(date_key)       AS month,
  date_format(date_key,'MMM') AS month_short,
  weekofyear(date_key)  AS week_of_year,
  day(date_key)         AS day_of_month,
  date_format(date_key,'E')  AS day_name,
  CASE WHEN date_format(date_key,'E') IN ('Sat','Sun') THEN 1 ELSE 0 END AS is_weekend
FROM calendar
"""
spark.sql(f"CREATE OR REPLACE TABLE {GOLD}.dim_date AS {_dim_date_sql}")
row_cnt = spark.table(f"{GOLD}.dim_date").count()
_distinct = spark.sql(f"SELECT COUNT(DISTINCT date_key) AS d FROM {GOLD}.dim_date").collect()[0].d
_record("dim_date", "rows", row_cnt)
_record("dim_date", "distinct_pk", _distinct)
_record("dim_date", "duplicate_pk_rows", row_cnt - _distinct)

# dim_claim (PK: claim_no) choose latest record per claim by analytics_created_timestamp
_drop_object(f"{GOLD}.dim_claim")
_dim_claim_sql = f"""
WITH src AS (
  SELECT * FROM {SILVER}.silver_smart_claims_analytics WHERE claim_no IS NOT NULL
), ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY claim_no ORDER BY analytics_created_timestamp DESC) rn FROM src
)
SELECT 
  claim_no,
  policy_no,
  chassis_no,
  claim_date,
  severity,
  severity_category,
  risk_category,
  processing_flag,
  suspicious_activity,
  premium,
  sum_insured,
  accident_telematics_distance_miles
FROM ranked WHERE rn = 1
"""
spark.sql(f"CREATE OR REPLACE TABLE {GOLD}.dim_claim AS {_dim_claim_sql}")
row_cnt = spark.table(f"{GOLD}.dim_claim").count()
_distinct = spark.sql(f"SELECT COUNT(DISTINCT claim_no) AS d FROM {GOLD}.dim_claim").collect()[0].d
_record("dim_claim", "rows", row_cnt)
_record("dim_claim", "distinct_pk", _distinct)
_record("dim_claim", "duplicate_pk_rows", row_cnt - _distinct)

# dim_policy (PK: policy_no)
_drop_object(f"{GOLD}.dim_policy")
_dim_policy_sql = f"""
SELECT DISTINCT
  policy_no,
  cust_id AS customer_id,
  driver_age,
  pol_issue_date,
  pol_expiry_date,
  premium,
  sum_insured
FROM {SILVER}.silver_smart_claims_analytics
WHERE policy_no IS NOT NULL
"""
spark.sql(f"CREATE OR REPLACE TABLE {GOLD}.dim_policy AS {_dim_policy_sql}")
row_cnt = spark.table(f"{GOLD}.dim_policy").count()
_distinct = spark.sql(f"SELECT COUNT(DISTINCT policy_no) AS d FROM {GOLD}.dim_policy").collect()[0].d
_record("dim_policy", "rows", row_cnt)
_record("dim_policy", "distinct_pk", _distinct)
_record("dim_policy", "duplicate_pk_rows", row_cnt - _distinct)

# dim_vehicle (PK: chassis_no)
_drop_object(f"{GOLD}.dim_vehicle")
_dim_vehicle_sql = f"""
SELECT DISTINCT
  chassis_no,
  make,
  model,
  model_year
FROM {SILVER}.silver_smart_claims_analytics
WHERE chassis_no IS NOT NULL
"""
spark.sql(f"CREATE OR REPLACE TABLE {GOLD}.dim_vehicle AS {_dim_vehicle_sql}")
row_cnt = spark.table(f"{GOLD}.dim_vehicle").count()
_distinct = spark.sql(f"SELECT COUNT(DISTINCT chassis_no) AS d FROM {GOLD}.dim_vehicle").collect()[0].d
_record("dim_vehicle", "rows", row_cnt)
_record("dim_vehicle", "distinct_pk", _distinct)
_record("dim_vehicle", "duplicate_pk_rows", row_cnt - _distinct)

# dim_location (composite PK: zip_code, latitude, longitude) - filter where any coordinate present
_drop_object(f"{GOLD}.dim_location")
_dim_location_sql = f"""
SELECT DISTINCT
  zip_code,
  borough,
  neighborhood,
  latitude,
  longitude
FROM {SILVER}.silver_smart_claims_analytics
WHERE zip_code IS NOT NULL OR (latitude IS NOT NULL AND longitude IS NOT NULL)
"""
spark.sql(f"CREATE OR REPLACE TABLE {GOLD}.dim_location AS {_dim_location_sql}")
row_cnt = spark.table(f"{GOLD}.dim_location").count()
_distinct = spark.sql(f"SELECT COUNT(DISTINCT struct(zip_code, latitude, longitude)) AS d FROM {GOLD}.dim_location").collect()[0].d
_record("dim_location", "rows", row_cnt)
_record("dim_location", "distinct_pk", _distinct)
_record("dim_location", "duplicate_pk_rows", row_cnt - _distinct)

# dim_risk (composite PK: severity_category, risk_category, processing_flag, speed_risk_indicator)
_drop_object(f"{GOLD}.dim_risk")
_dim_risk_sql = f"""
SELECT DISTINCT
  severity_category,
  risk_category,
  processing_flag,
  speed_risk_indicator
FROM {SILVER}.silver_smart_claims_analytics
"""
spark.sql(f"CREATE OR REPLACE TABLE {GOLD}.dim_risk AS {_dim_risk_sql}")
row_cnt = spark.table(f"{GOLD}.dim_risk").count()
_distinct = spark.sql(f"SELECT COUNT(DISTINCT struct(severity_category, risk_category, processing_flag, speed_risk_indicator)) AS d FROM {GOLD}.dim_risk").collect()[0].d
_record("dim_risk", "rows", row_cnt)
_record("dim_risk", "distinct_pk", _distinct)
_record("dim_risk", "duplicate_pk_rows", row_cnt - _distinct)

# Facts

# fact_claims
_drop_object(f"{GOLD}.fact_claims")
_fact_claims_sql = f"""
SELECT 
  claim_no,
  policy_no,
  chassis_no,
  claim_date,
  claim_amount_total,
  claim_amount_vehicle,
  claim_amount_injury,
  claim_amount_property,
  premium,
  sum_insured,
  severity,
  severity_category,
  risk_category,
  processing_flag
FROM {SILVER}.silver_smart_claims_analytics
"""
spark.sql(f"CREATE OR REPLACE TABLE {GOLD}.fact_claims AS {_fact_claims_sql}")
_record("fact_claims", "rows", spark.table(f"{GOLD}.fact_claims").count())

# fact_telematics
_drop_object(f"{GOLD}.fact_telematics")
_fact_telematics_sql = f"""
SELECT 
  chassis_no,
  claim_no,
  telematics_speed,
  telematics_speed_std,
  telematics_latitude,
  telematics_longitude,
  accident_telematics_distance_miles,
  speed_risk_indicator
FROM {SILVER}.silver_smart_claims_analytics
"""
spark.sql(f"CREATE OR REPLACE TABLE {GOLD}.fact_telematics AS {_fact_telematics_sql}")
_record("fact_telematics", "rows", spark.table(f"{GOLD}.fact_telematics").count())

# fact_accident_severity
_drop_object(f"{GOLD}.fact_accident_severity")
_fact_accident_severity_sql = f"""
SELECT 
  claim_no,
  chassis_no,
  severity,
  severity_category,
  risk_category,
  processing_flag,
  data_source,
  analytics_created_timestamp
FROM {SILVER}.silver_smart_claims_analytics
"""
spark.sql(f"CREATE OR REPLACE TABLE {GOLD}.fact_accident_severity AS {_fact_accident_severity_sql}")
_record("fact_accident_severity", "rows", spark.table(f"{GOLD}.fact_accident_severity").count())

# fact_rules (only if gold_insights exists)
if table_exists(f"{GOLD}.gold_insights"):
    _drop_object(f"{GOLD}.fact_rules")
    _fact_rules_sql = f"""
    SELECT 
      claim_no,
      policy_no,
      chassis_no,
      valid_date,
      valid_amount,
      rules_processed_timestamp
    FROM {GOLD}.gold_insights
    """
    spark.sql(f"CREATE OR REPLACE TABLE {GOLD}.fact_rules AS {_fact_rules_sql}")
    _record("fact_rules", "rows", spark.table(f"{GOLD}.fact_rules").count())
else:
    print("⚠️ Skipping fact_rules (gold.gold_insights not found yet)")

# fact_smart_claims (full wide table)
_drop_object(f"{GOLD}.fact_smart_claims")
_fact_smart_claims_sql = f"SELECT * FROM {SILVER}.silver_smart_claims_analytics"
spark.sql(f"CREATE OR REPLACE TABLE {GOLD}.fact_smart_claims AS {_fact_smart_claims_sql}")
_record("fact_smart_claims", "rows", spark.table(f"{GOLD}.fact_smart_claims").count())

# Referential integrity checks (logical only)
ri_checks = [
    ("fact_claims", "claim_no", "dim_claim", "claim_no"),
    ("fact_claims", "policy_no", "dim_policy", "policy_no"),
    ("fact_claims", "chassis_no", "dim_vehicle", "chassis_no"),
    ("fact_telematics", "claim_no", "dim_claim", "claim_no"),
    ("fact_telematics", "chassis_no", "dim_vehicle", "chassis_no"),
    ("fact_accident_severity", "claim_no", "dim_claim", "claim_no"),
    ("fact_accident_severity", "chassis_no", "dim_vehicle", "chassis_no"),
]
if table_exists(f"{GOLD}.fact_rules"):
    ri_checks.extend([
        ("fact_rules", "claim_no", "dim_claim", "claim_no"),
        ("fact_rules", "policy_no", "dim_policy", "policy_no"),
        ("fact_rules", "chassis_no", "dim_vehicle", "chassis_no"),
    ])

for fact, fcol, dim, dcol in ri_checks:
    missing = spark.sql(f"SELECT COUNT(*) AS c FROM {GOLD}.{fact} f LEFT ANTI JOIN {GOLD}.{dim} d ON f.{fcol}=d.{dcol}").collect()[0].c
    _record(fact, f"orphan_fk_{fcol}_to_{dim}", missing)

# Build constraint audit dataframe
from pyspark.sql.types import StructType, StructField, StringType, LongType

schema = StructType([
    StructField("entity", StringType(), True),
    StructField("metric", StringType(), True),
    StructField("value", StringType(), True),
])
metrics_rows = [(e, m, str(v)) for e, m, v in metrics]
audit_df = spark.createDataFrame(metrics_rows, schema)

_drop_object(f"{GOLD}.constraint_audit")
audit_df.write.mode("overwrite").format("delta").saveAsTable(f"{GOLD}.constraint_audit")
print("✅ Created gold.constraint_audit with" , audit_df.count(), "rows")
audit_df.show(truncate=False)

# -----------------------------------------------------------------------------
# Aggregated reporting views (keep as views/materialized as before)
# -----------------------------------------------------------------------------
print("🛠 Building reporting summary views ...")

# We retain helper for views

def _drop_any_view(name: str):
    for kind in ["MATERIALIZED LAKE VIEW", "MATERIALIZED VIEW", "VIEW"]:
        try:
            spark.sql(f"DROP {kind} IF EXISTS {name}")
        except Exception:
            pass

def create_materialized(name: str, body_sql: str):
    _drop_any_view(name)
    attempts = [
        ("MATERIALIZED LAKE VIEW", f"CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS {name} AS {body_sql}"),
        ("MATERIALIZED VIEW", f"CREATE MATERIALIZED VIEW IF NOT EXISTS {name} AS {body_sql}"),
        ("VIEW", f"CREATE OR REPLACE VIEW {name} AS {body_sql}"),
    ]
    for kind, stmt in attempts:
        try:
            spark.sql(stmt)
            print(f"✅ Created {kind.lower()} {name}")
            return
        except Exception:
            continue
    print(f"❌ Failed to create view {name}")

_v_claims_summary_body = f"""
SELECT 
  risk_category,
  processing_flag,
  COUNT(*) AS claim_count,
  AVG(claim_amount_total) AS avg_claim_amount,
  AVG(severity) AS avg_severity,
  AVG(telematics_speed) AS avg_speed,
  SUM(claim_amount_total) AS total_exposure
FROM {SILVER}.silver_smart_claims_analytics
GROUP BY risk_category, processing_flag
"""
create_materialized(f"{GOLD}.v_claims_summary_by_risk", _v_claims_summary_body)

_v_dashboard_body = f"""
SELECT 
    claim_no,
    chassis_no,
    policy_no,
    cust_id,
    make,
    model,
    claim_amount_total,
    claim_amount_vehicle,
    claim_amount_injury,
    claim_amount_property,
    sum_insured,
    premium,
    severity,
    suspicious_activity,
    risk_category,
    processing_flag,
    telematics_speed,
    speed_risk_indicator,
    accident_telematics_distance_miles,
    borough,
    neighborhood,
    zip_code,
    claim_date,
    analytics_created_timestamp 
FROM {SILVER}.silver_smart_claims_analytics
"""
create_materialized(f"{GOLD}.v_smart_claims_dashboard", _v_dashboard_body)

print("🏁 Gold view build complete (dimensions/facts as tables with audit)")
