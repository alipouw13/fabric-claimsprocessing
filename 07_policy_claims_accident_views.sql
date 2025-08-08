#!/usr/bin/env python
# coding: utf-8

# ## NB_4b_iot_sql
# 
# New notebook

# In[1]:


-- Set up Spark SQL configuration for Fabric Lakehouse
SET spark.sql.adaptive.enabled = true;
SET spark.sql.adaptive.coalescePartitions.enabled = true;
SET spark.sql.adaptive.skewJoin.enabled = true;


# In[3]:


-- Validate source tables exist
SHOW TABLES LIKE 'silver_*';

-- COMMAND ----------
-- Check data availability in source tables
SELECT 
    'silver_claim_policy_location' as table_name,
    COUNT(*) as row_count,
    NULL as unique_chassis,
    COUNT(DISTINCT claim_no) as unique_claims
FROM silver_claim_policy_location

UNION ALL

SELECT 
    'silver_telematics' as table_name,
    COUNT(*) as row_count,
    COUNT(DISTINCT chassis_no) as unique_chassis,
    NULL as unique_claims
FROM silver_telematics

UNION ALL

SELECT 
    'silver_accident' as table_name,
    COUNT(*) as row_count,
    NULL as unique_chassis,
    COUNT(DISTINCT claim_no) as unique_claims
FROM silver_accident;


# In[4]:


-- Create table with averaged telematics data per vehicle
CREATE OR REPLACE TABLE silver_claim_policy_telematics_avg
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
    FROM silver_claim_policy_location AS p_c 
    INNER JOIN (
        SELECT 
            chassis_no,
            ROUND(AVG(speed), 2) AS telematics_speed,
            ROUND(AVG(latitude), 6) AS telematics_latitude,
            ROUND(AVG(longitude), 6) AS telematics_longitude,
            ROUND(STDDEV(speed), 2) AS telematics_speed_std,
            COUNT(*) AS telematics_record_count,
            MAX(event_timestamp) AS telematics_latest_timestamp
        FROM silver_telematics
        WHERE chassis_no IS NOT NULL
          AND speed IS NOT NULL
          AND latitude IS NOT NULL
          AND longitude IS NOT NULL
        GROUP BY chassis_no
    ) t ON p_c.chassis_no = t.chassis_no
);


# In[6]:


-- Create table with detailed telematics data (all events)
CREATE OR REPLACE TABLE silver_claim_policy_telematics
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
    FROM silver_telematics AS t
    INNER JOIN silver_claim_policy_location AS p_c 
        ON p_c.chassis_no = t.chassis_no
    WHERE t.chassis_no IS NOT NULL
      AND t.event_timestamp IS NOT NULL
);


# In[8]:


-- Create integrated table with accident data
-- Note: Uncommented from original Databricks version for Fabric compatibility
CREATE OR REPLACE TABLE silver_claim_policy_accident
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
    FROM silver_claim_policy_telematics_avg AS p_c 
    LEFT JOIN silver_accident AS a
        ON p_c.claim_no = a.claim_no
);


# In[14]:


-- Create comprehensive analytics table
-- Create comprehensive analytics table
CREATE OR REPLACE TABLE silver_smart_claims_analytics
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
        
    FROM silver_claim_policy_accident AS pca
    WHERE pca.claim_no IS NOT NULL
      AND pca.chassis_no IS NOT NULL
);



# In[15]:


-- Validate the integrated data
SELECT 
    'silver_claim_policy_telematics_avg' AS table_name,
    COUNT(*) AS total_records,
    COUNT(DISTINCT chassis_no) AS unique_vehicles,
    COUNT(DISTINCT claim_no) AS unique_claims,
    AVG(telematics_speed) AS avg_speed,
    MIN(telematics_latest_timestamp) AS earliest_telematics,
    MAX(telematics_latest_timestamp) AS latest_telematics
FROM silver_claim_policy_telematics_avg

UNION ALL

SELECT 
    'silver_claim_policy_telematics' AS table_name,
    COUNT(*) AS total_records,
    COUNT(DISTINCT chassis_no) AS unique_vehicles,
    COUNT(DISTINCT claim_no) AS unique_claims,
    AVG(telematics_speed) AS avg_speed,
    MIN(telematics_timestamp) AS earliest_telematics,
    MAX(telematics_timestamp) AS latest_telematics
FROM silver_claim_policy_telematics

UNION ALL

SELECT 
    'silver_claim_policy_accident' AS table_name,
    COUNT(*) AS total_records,
    COUNT(DISTINCT chassis_no) AS unique_vehicles,
    COUNT(DISTINCT claim_no) AS unique_claims,
    AVG(CAST(severity AS DOUBLE)) AS avg_severity,
    MIN(accident_processing_timestamp) AS earliest_processing,
    MAX(accident_processing_timestamp) AS latest_processing
FROM silver_claim_policy_accident;


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
FROM silver_smart_claims_analytics


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
FROM silver_smart_claims_analytics
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

