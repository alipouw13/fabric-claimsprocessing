#!/usr/bin/env python
# coding: utf-8

# ## NB_4b_iot_sql
# 
# New notebook

# In[1]:

-- Ensure silver schema exists
CREATE SCHEMA IF NOT EXISTS silver;

-- Set up Spark SQL configuration for Fabric Lakehouse
SET spark.sql.adaptive.enabled = true;
SET spark.sql.adaptive.coalescePartitions.enabled = true;
SET spark.sql.adaptive.skewJoin.enabled = true;


# In[3]:

-- Validate source tables exist in silver schema
SHOW TABLES IN silver LIKE 'silver_%';

-- COMMAND ----------
-- Check data availability in source tables
SELECT 
    'silver.silver_claim_policy_location' as table_name,
    COUNT(*) as row_count,
    NULL as unique_chassis,
    COUNT(DISTINCT claim_no) as unique_claims
FROM silver.silver_claim_policy_location

UNION ALL

SELECT 
    'silver.silver_telematics' as table_name,
    COUNT(*) as row_count,
    COUNT(DISTINCT chassis_no) as unique_chassis,
    NULL as unique_claims
FROM silver.silver_telematics

UNION ALL

SELECT 
    'silver.silver_accident' as table_name,
    COUNT(*) as row_count,
    NULL as unique_chassis,
    COUNT(DISTINCT claim_no) as unique_claims
FROM silver.silver_accident;


# In[4]:

-- Create table with averaged telematics data per vehicle (in silver schema)
CREATE OR REPLACE TABLE silver.silver_claim_policy_telematics_avg
USING DELTA
AS (
    SELECT 
        p_c.*,
        t.telematics_latitude,
        t.telematics_longitude,
        t.telematics_speed,
        t.telematics_record_count,
        t.telematics_speed_std,
        t.telematics_latest_timestamp
    FROM silver.silver_claim_policy_location AS p_c 
    INNER JOIN (
        SELECT 
            chassis_no,
            ROUND(AVG(speed), 2) AS telematics_speed,
            ROUND(AVG(latitude), 6) AS telematics_latitude,
            ROUND(AVG(longitude), 6) AS telematics_longitude,
            ROUND(STDDEV(speed), 2) AS telematics_speed_std,
            COUNT(*) AS telematics_record_count,
            MAX(event_timestamp) AS telematics_latest_timestamp
        FROM silver.silver_telematics
        WHERE chassis_no IS NOT NULL
          AND speed IS NOT NULL
          AND latitude IS NOT NULL
          AND longitude IS NOT NULL
        GROUP BY chassis_no
    ) t ON p_c.chassis_no = t.chassis_no
);


# In[6]:

-- Create table with detailed telematics data (all events) in silver schema
CREATE OR REPLACE TABLE silver.silver_claim_policy_telematics
USING DELTA
AS (
    SELECT 
        p_c.*,
        t.latitude AS telematics_latitude,
        t.longitude AS telematics_longitude,
        t.event_timestamp AS telematics_timestamp,
        t.speed AS telematics_speed,
        -- Calculate time-based features
        EXTRACT(HOUR FROM t.event_timestamp) AS telematics_hour,
        EXTRACT(DAYOFWEEK FROM t.event_timestamp) AS telematics_day_of_week,
        -- Add data quality indicators
        CASE 
            WHEN t.speed > 200 THEN 'ANOMALY_HIGH_SPEED'
            WHEN t.speed < 0 THEN 'ANOMALY_NEGATIVE_SPEED'
            WHEN t.latitude < -90 OR t.latitude > 90 THEN 'ANOMALY_INVALID_LAT'
            WHEN t.longitude < -180 OR t.longitude > 180 THEN 'ANOMALY_INVALID_LON'
            ELSE 'VALID'
        END AS data_quality_flag
    FROM silver.silver_telematics AS t
    INNER JOIN silver.silver_claim_policy_location AS p_c 
        ON p_c.chassis_no = t.chassis_no
    WHERE t.chassis_no IS NOT NULL
      AND t.event_timestamp IS NOT NULL
);


# In[8]:

-- Create integrated table with accident data in silver schema
CREATE OR REPLACE TABLE silver.silver_claim_policy_accident
USING DELTA
AS (
    SELECT 
        p_c.*,
        a.severity,
        a.severity_category,
        a.image_name,
        a.processed_timestamp AS accident_processing_timestamp,
        a.data_quality_score AS accident_data_quality,
        -- Calculate risk indicators
        CASE 
            WHEN a.severity >= 0.8 THEN 'HIGH_RISK'
            WHEN a.severity >= 0.5 THEN 'MEDIUM_RISK'
            WHEN a.severity >= 0.2 THEN 'LOW_RISK'
            ELSE 'MINIMAL_RISK'
        END AS risk_category,
        -- Add business logic flags
        CASE 
            WHEN a.severity > 0.7 AND p_c.premium < 1000 THEN 'REVIEW_REQUIRED'
            WHEN a.severity > 0.9 THEN 'TOTAL_LOSS_CANDIDATE'
            ELSE 'STANDARD_PROCESSING'
        END AS processing_flag
    FROM silver.silver_claim_policy_telematics_avg AS p_c 
    LEFT JOIN silver.silver_accident AS a
        ON p_c.claim_no = a.claim_no
);


# In[14]:

-- Create comprehensive analytics table in silver schema
CREATE OR REPLACE TABLE silver.silver_smart_claims_analytics
USING DELTA
AS (
    SELECT 
        -- Core identifiers
        pca.claim_no,
        pca.CHASSIS_NO,
        pca.policy_no,
        -- Policy information
        pca.CUST_ID,
        pca.driver_age,
        pca.premium,
        pca.pol_issue_date,
        pca.pol_expiry_date,
        pca.MAKE,
        pca.MODEL,
        pca.MODEL_YEAR,
        pca.suspicious_activity,
        pca.SUM_INSURED,
        -- Claim information
        pca.claim_date,
        pca.claim_amount_total,
        pca.claim_amount_vehicle,
        pca.claim_amount_injury,
        pca.claim_amount_property,
        -- Location information
        pca.latitude,
        pca.longitude,
        pca.BOROUGH,
        pca.NEIGHBORHOOD,
        pca.ZIP_CODE,
        -- Accident severity
        pca.incident_severity,
        pca.severity,
        pca.risk_category,
        pca.processing_flag,
        -- Telematics aggregates
        pca.telematics_latitude,
        pca.telematics_longitude,
        pca.telematics_speed,
        pca.telematics_speed_std,
        -- Calculated risk indicators
        CASE 
            WHEN pca.telematics_speed > 80 AND pca.severity > 0.6 THEN 'HIGH_SPEED_SEVERE'
            WHEN pca.telematics_speed > 60 AND pca.severity > 0.4 THEN 'MODERATE_SPEED_RISK'
            ELSE 'NORMAL_RISK'
        END AS speed_risk_indicator,
        -- Distance calculations (approximate)
        SQRT(
            POW((pca.latitude - pca.telematics_latitude) * 69, 2) + 
            POW((pca.longitude - pca.telematics_longitude) * 54.6, 2)
        ) AS accident_telematics_distance_miles,
        -- Processing metadata
        CURRENT_TIMESTAMP() AS analytics_created_timestamp,
        'fabric_lakehouse' AS data_source
    FROM silver.silver_claim_policy_accident AS pca
    WHERE pca.claim_no IS NOT NULL
      AND pca.chassis_no IS NOT NULL
);


# In[15]:

-- Validate the integrated data in silver schema
SELECT 
    'silver.silver_claim_policy_telematics_avg' AS table_name,
    COUNT(*) AS total_records,
    COUNT(DISTINCT chassis_no) AS unique_vehicles,
    COUNT(DISTINCT claim_no) AS unique_claims,
    AVG(telematics_speed) AS avg_speed,
    MIN(telematics_latest_timestamp) AS earliest_telematics,
    MAX(telematics_latest_timestamp) AS latest_telematics
FROM silver.silver_claim_policy_telematics_avg

UNION ALL

SELECT 
    'silver.silver_claim_policy_telematics' AS table_name,
    COUNT(*) AS total_records,
    COUNT(DISTINCT chassis_no) AS unique_vehicles,
    COUNT(DISTINCT claim_no) AS unique_claims,
    AVG(telematics_speed) AS avg_speed,
    MIN(telematics_timestamp) AS earliest_telematics,
    MAX(telematics_timestamp) AS latest_telematics
FROM silver.silver_claim_policy_telematics

UNION ALL

SELECT 
    'silver.silver_claim_policy_accident' AS table_name,
    COUNT(*) AS total_records,
    COUNT(DISTINCT chassis_no) AS unique_vehicles,
    COUNT(DISTINCT claim_no) AS unique_claims,
    AVG(CAST(severity AS DOUBLE)) AS avg_severity,
    MIN(accident_processing_timestamp) AS earliest_processing,
    MAX(accident_processing_timestamp) AS latest_processing
FROM silver.silver_claim_policy_accident;


# In[ ]:


-- OPTIMIZE silver_claim_policy_telematics_avg ZORDER BY (chassis_no, claim_no);
-- OPTIMIZE silver_claim_policy_telematics ZORDER BY (chassis_no, telematics_timestamp);
-- OPTIMIZE silver_claim_policy_accident ZORDER BY (claim_no, severity);
-- OPTIMIZE silver_smart_claims_analytics ZORDER BY (claim_no, chassis_no);


# In[29]:


-- Create view for Power BI consumption
DROP MATERIALIZED LAKE VIEW IF EXISTS v_smart_claims_dashboard;
CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS v_smart_claims_dashboard AS 
SELECT 
    claim_no,
    CHASSIS_NO,
    CUST_ID,
    MAKE,
    MODEL,
    claim_amount_total,
    claim_amount_vehicle,
    claim_amount_injury,
    claim_amount_property,
    SUM_INSURED,
    premium,
    severity,
    suspicious_activity,
    risk_category,
    processing_flag,
    telematics_speed,
    speed_risk_indicator,
    accident_telematics_distance_miles,
    BOROUGH,
    NEIGHBORHOOD,
    ZIP_CODE,
    claim_date,
    analytics_created_timestamp 
FROM silver.silver_smart_claims_analytics;


# In[30]:


-- Create aggregated summary view
CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS v_claims_summary_by_risk AS 
SELECT 
    risk_category,
    processing_flag,
    COUNT(*) AS claim_count,
    AVG(claim_amount_total) AS avg_claim_amount,
    AVG(severity) AS avg_severity,
    AVG(telematics_speed) AS avg_speed,
    SUM(claim_amount_total) AS total_exposure
FROM silver.silver_smart_claims_analytics
GROUP BY risk_category, processing_flag
ORDER BY total_exposure DESC;


# In[31]:


DESCRIBE EXTENDED silver_claim_policy_telematics_avg;


# In[32]:


DESCRIBE EXTENDED silver_claim_policy_accident;


# In[33]:


DESCRIBE EXTENDED silver_smart_claims_analytics;


# In[34]:


SELECT 
    'EXECUTION COMPLETED' AS status,
    CURRENT_TIMESTAMP() AS completion_time,
    'All tables created and optimized for Fabric Lakehouse' AS message;


# In[35]:


-- Ensure gold schema exists
CREATE SCHEMA IF NOT EXISTS gold;

-- =============================
-- GOLD DIMENSION VIEWS
-- =============================
DROP MATERIALIZED LAKE VIEW IF EXISTS gold.dim_date;
CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS gold.dim_date AS
WITH bounds AS (
  SELECT date(min(claim_date)) AS min_d, date(max(claim_date)) AS max_d FROM silver.silver_smart_claims_analytics
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
FROM calendar;

DROP MATERIALIZED LAKE VIEW IF EXISTS gold.dim_claim;
CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS gold.dim_claim AS
SELECT DISTINCT
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
  SUM_INSURED AS sum_insured,
  accident_telematics_distance_miles
FROM silver.silver_smart_claims_analytics
WHERE claim_no IS NOT NULL;

DROP MATERIALIZED LAKE VIEW IF EXISTS gold.dim_policy;
CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS gold.dim_policy AS
SELECT DISTINCT
  policy_no,
  CUST_ID AS customer_id,
  driver_age,
  pol_issue_date,
  pol_expiry_date,
  premium,
  SUM_INSURED AS sum_insured
FROM silver.silver_smart_claims_analytics
WHERE policy_no IS NOT NULL;

DROP MATERIALIZED LAKE VIEW IF EXISTS gold.dim_vehicle;
CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS gold.dim_vehicle AS
SELECT DISTINCT
  chassis_no,
  MAKE,
  MODEL,
  MODEL_YEAR
FROM silver.silver_smart_claims_analytics
WHERE chassis_no IS NOT NULL;

DROP MATERIALIZED LAKE VIEW IF EXISTS gold.dim_location;
CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS gold.dim_location AS
SELECT DISTINCT
  ZIP_CODE,
  BOROUGH,
  NEIGHBORHOOD,
  latitude,
  longitude
FROM silver.silver_smart_claims_analytics
WHERE ZIP_CODE IS NOT NULL OR (latitude IS NOT NULL AND longitude IS NOT NULL);

DROP MATERIALIZED LAKE VIEW IF EXISTS gold.dim_risk;
CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS gold.dim_risk AS
SELECT DISTINCT
  severity_category,
  risk_category,
  processing_flag,
  speed_risk_indicator
FROM silver.silver_smart_claims_analytics;

-- =============================
-- GOLD FACT VIEWS
-- =============================
DROP MATERIALIZED LAKE VIEW IF EXISTS gold.fact_claims;
CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS gold.fact_claims AS
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
  SUM_INSURED AS sum_insured,
  severity,
  severity_category,
  risk_category,
  processing_flag
FROM silver.silver_smart_claims_analytics;

DROP MATERIALIZED LAKE VIEW IF EXISTS gold.fact_telematics;
CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS gold.fact_telematics AS
SELECT 
  chassis_no,
  claim_no,
  telematics_speed,
  telematics_speed_std,
  telematics_latitude,
  telematics_longitude,
  accident_telematics_distance_miles,
  speed_risk_indicator
FROM silver.silver_smart_claims_analytics;

DROP MATERIALIZED LAKE VIEW IF EXISTS gold.fact_accident_severity;
CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS gold.fact_accident_severity AS
SELECT 
  claim_no,
  chassis_no,
  severity,
  severity_category,
  risk_category,
  processing_flag,
  data_source,
  analytics_created_timestamp
FROM silver.silver_smart_claims_analytics;

-- Rules / outcomes fact (requires gold.gold_insights produced by rules engine)
DROP MATERIALIZED LAKE VIEW IF EXISTS gold.fact_rules;
CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS gold.fact_rules AS
SELECT 
  claim_no,
  policy_no,
  chassis_no,
  valid_date,
  valid_amount,
  reported_severity_check,
  release_funds,
  rules_processed_timestamp
FROM gold.gold_insights;

-- Wide integrated fact (single-table model option)
DROP MATERIALIZED LAKE VIEW IF EXISTS gold.fact_smart_claims;
CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS gold.fact_smart_claims AS
SELECT * FROM silver.silver_smart_claims_analytics;

-- =============================
-- DASHBOARD FRIENDLY AGGREGATIONS
-- =============================
DROP MATERIALIZED LAKE VIEW IF EXISTS gold.v_claims_summary_by_risk;
CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS gold.v_claims_summary_by_risk AS
SELECT 
  risk_category,
  processing_flag,
  COUNT(*) AS claim_count,
  AVG(claim_amount_total) AS avg_claim_amount,
  AVG(severity) AS avg_severity,
  AVG(telematics_speed) AS avg_speed,
  SUM(claim_amount_total) AS total_exposure
FROM silver.silver_smart_claims_analytics
GROUP BY risk_category, processing_flag;

DROP MATERIALIZED LAKE VIEW IF EXISTS gold.v_smart_claims_dashboard;
CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS gold.v_smart_claims_dashboard AS
SELECT 
    claim_no,
    chassis_no,
    policy_no,
    CUST_ID,
    MAKE,
    MODEL,
    claim_amount_total,
    claim_amount_vehicle,
    claim_amount_injury,
    claim_amount_property,
    SUM_INSURED,
    premium,
    severity,
    suspicious_activity,
    risk_category,
    processing_flag,
    telematics_speed,
    speed_risk_indicator,
    accident_telematics_distance_miles,
    BOROUGH,
    NEIGHBORHOOD,
    ZIP_CODE,
    claim_date,
    analytics_created_timestamp 
FROM silver.silver_smart_claims_analytics;

