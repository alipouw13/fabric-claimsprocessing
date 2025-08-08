# Microsoft Fabric notebook source
# This notebook has been adapted from the original Databricks version
# Available at https://github.com/databricks-industry-solutions/smart-claims

# COMMAND ----------

# Configure paths for Microsoft Fabric (OneLake or ADLS Gen2)
# Update these paths to match your Fabric workspace structure
lakehouse_name = "your_lakehouse_name"  # Replace with your actual lakehouse name

claims_path = f"abfss://your_workspace@onelake.dfs.fabric.microsoft.com/{lakehouse_name}/Files/data_sources/Claims"
policy_path = f"abfss://your_workspace@onelake.dfs.fabric.microsoft.com/{lakehouse_name}/Files/data_sources/Policy/policies.csv"
accident_path = f"abfss://your_workspace@onelake.dfs.fabric.microsoft.com/{lakehouse_name}/Files/data_sources/Accidents"

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id, lit, row_number, current_date
from pyspark.sql.window import Window
from pyspark.sql import types as T
from pyspark.sql import functions as F
from delta.tables import DeltaTable
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

def flatten(df):
    """
    Flatten nested structures in a DataFrame
    """
    complex_fields = dict([
        (field.name, field.dataType) 
        for field in df.schema.fields 
        if isinstance(field.dataType, T.ArrayType) or isinstance(field.dataType, T.StructType)
    ])
    
    while len(complex_fields) != 0:
        col_name = list(complex_fields.keys())[0]
        
        if isinstance(complex_fields[col_name], T.StructType):
            expanded = [F.col(col_name + '.' + k).alias(col_name + '_' + k) 
                        for k in [n.name for n in complex_fields[col_name]]
                       ]
            
            df = df.select("*", *expanded).drop(col_name)
    
        elif isinstance(complex_fields[col_name], T.ArrayType): 
            df = df.withColumn(col_name, F.explode(col_name))
    
        complex_fields = dict([
            (field.name, field.dataType)
            for field in df.schema.fields
            if isinstance(field.dataType, T.ArrayType) or isinstance(field.dataType, T.StructType)
        ])

    return df

# COMMAND ----------

def apply_data_quality_checks(df, expectations, drop_invalid=True):
    """
    Apply data quality checks similar to DLT expectations
    """
    if drop_invalid:
        # Apply all filters to drop invalid records
        for check_name, condition in expectations.items():
            initial_count = df.count()
            df = df.filter(condition)
            final_count = df.count()
            dropped_count = initial_count - final_count
            
            if dropped_count > 0:
                logger.warning(f"Data quality check '{check_name}': Dropped {dropped_count} invalid records")
            else:
                logger.info(f"Data quality check '{check_name}': All records passed")
    
    return df

# COMMAND ----------

def write_delta_table(df, table_name, mode="overwrite", optimize=True):
    """
    Write DataFrame to Delta table with optimization
    """
    try:
        # Write to Delta table
        df.write \
          .format("delta") \
          .mode(mode) \
          .option("overwriteSchema", "true") \
          .saveAsTable(table_name)
        
        # Optimize the table
        if optimize:
            spark.sql(f"OPTIMIZE {table_name}")
            logger.info(f"Table {table_name} created and optimized successfully")
        
        return True
    except Exception as e:
        logger.error(f"Error writing table {table_name}: {str(e)}")
        return False


# COMMAND ----------

def create_silver_policy():
    """
    Create silver policies table with data quality checks and transformations
    """
    try:
        logger.info("Creating silver_policy table...")
        
        # Read bronze policy data
        staged_policy = spark.table("bronze_policy")
        
        # Apply transformations
        silver_policy = staged_policy.withColumn("premium", F.abs(F.col("premium"))) \
            .withColumn("pol_eff_date", F.to_date(F.col("pol_eff_date"), "dd-MM-yyyy")) \
            .withColumn("pol_expiry_date", F.to_date(F.col("pol_expiry_date"), "dd-MM-yyyy")) \
            .withColumn("pol_issue_date", F.to_date(F.col("pol_issue_date"), "dd-MM-yyyy"))
        
        # Define data quality expectations
        expectations = {
            "valid_sum_insured": "sum_insured > 0",
            "valid_policy_number": "policy_no IS NOT NULL",
            "valid_premium": "premium > 1",
            "valid_issue_date": "pol_issue_date < current_date()",
            "valid_effective_date": "pol_eff_date < current_date()",
            "valid_expiry_date": "pol_expiry_date <= current_date()",
            "valid_model_year": "model_year > 0"
        }
        
        # Apply data quality checks
        silver_policy = apply_data_quality_checks(silver_policy, expectations, drop_invalid=True)
        
        # Add processing metadata
        silver_policy = silver_policy.withColumn("processed_timestamp", F.current_timestamp())
        
        # Write to Delta table
        success = write_delta_table(silver_policy, "silver_policy", mode="overwrite")
        
        if success:
            logger.info(f"Silver policy table created with {silver_policy.count()} records")
            return silver_policy
        else:
            raise Exception("Failed to create silver_policy table")
            
    except Exception as e:
        logger.error(f"Error creating silver_policy: {str(e)}")
        raise

# Execute silver policy creation
silver_policy_df = create_silver_policy()

# COMMAND ----------

def create_silver_claim():
    """
    Create silver claims table with data quality checks and transformations
    """
    try:
        logger.info("Creating silver_claim table...")
        
        # Read bronze claim data
        staged_claim = spark.table("bronze_claim")
        
        # Flatten nested structures
        curated_claim = flatten(staged_claim)
        
        # Apply transformations
        silver_claim = curated_claim \
            .withColumn("claim_date", F.to_date(F.col("claim_date"))) \
            .withColumn("incident_date", F.to_date(F.col("incident_date"), "yyyy-MM-dd")) \
            .withColumn("driver_license_issue_date", F.to_date(F.col("driver_license_issue_date"), "dd-MM-yyyy"))
        
        # Define data quality expectations
        expectations = {
            "valid_claim_date": "claim_date < current_date()",
            "valid_incident_date": "incident_date < current_date()",
            "valid_incident_hour": "incident_hour between 0 and 24",
            "valid_driver_age": "driver_age > 16",
            "valid_driver_license": "driver_license_issue_date > (current_date() - cast(cast(driver_age AS INT) AS INTERVAL YEAR))",
            "valid_claim_amount": "claim_amount_total > 0"
        }
        
        # Apply data quality checks
        silver_claim = apply_data_quality_checks(silver_claim, expectations, drop_invalid=True)
        
        # Add processing metadata
        silver_claim = silver_claim.withColumn("processed_timestamp", F.current_timestamp())
        
        # Write to Delta table
        success = write_delta_table(silver_claim, "silver_claim", mode="overwrite")
        
        if success:
            logger.info(f"Silver claim table created with {silver_claim.count()} records")
            return silver_claim
        else:
            raise Exception("Failed to create silver_claim table")
            
    except Exception as e:
        logger.error(f"Error creating silver_claim: {str(e)}")
        raise

# Execute silver claim creation
silver_claim_df = create_silver_claim()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer - Business Ready Data

# COMMAND ----------

def create_silver_claim_policy():
    """
    Create joined claims and policy table
    """
    try:
        logger.info("Creating silver_claim_policy table...")
        
        # Read silver tables
        silver_claim = spark.table("silver_claim")
        silver_policy = spark.table("silver_policy")
        
        # Rename conflicting columns in policy table to avoid duplicates
        policy_renamed = silver_policy \
            .withColumnRenamed("load_timestamp", "policy_load_timestamp") \
            .withColumnRenamed("source_file", "policy_source_file") \
            .withColumnRenamed("processed_timestamp", "policy_processed_timestamp")
        
        # Join tables
        joined_df = silver_claim.join(policy_renamed, on="policy_no", how="inner")
        
        # Define data quality expectations for joined data
        expectations = {
            "valid_claim_number": "claim_no IS NOT NULL",
            "valid_policy_number": "policy_no IS NOT NULL",
            "valid_effective_date": "pol_eff_date < current_date()",
            "valid_expiry_date": "pol_expiry_date <= current_date()"
        }
        
        # Apply data quality checks (but don't drop, just log)
        for check_name, condition in expectations.items():
            invalid_count = joined_df.filter(f"NOT ({condition})").count()
            if invalid_count > 0:
                logger.warning(f"Data quality check '{check_name}': Found {invalid_count} invalid records")
            else:
                logger.info(f"Data quality check '{check_name}': All records passed")
        
        # Add processing metadata
        joined_df = joined_df.withColumn("join_processed_timestamp", F.current_timestamp())
        
        # Write to Delta table
        success = write_delta_table(joined_df, "silver_claim_policy", mode="overwrite")
        
        if success:
            logger.info(f"Silver claim policy table created with {joined_df.count()} records")
            return joined_df
        else:
            raise Exception("Failed to create silver_claim_policy table")
            
    except Exception as e:
        logger.error(f"Error creating silver_claim_policy: {str(e)}")
        raise

# Execute silver claim policy creation
silver_claim_policy_df = create_silver_claim_policy()

# COMMAND ----------

# Display summary of all created tables
print("="*50)
print("DATA PIPELINE EXECUTION SUMMARY")
print("="*50)

tables_info = [
    ("silver_claim", "Curated and validated claims data"),
    ("silver_policy", "Curated and validated policy data"),
    ("silver_claim_policy", "Joined claims and policy data")
]

for table_name, description in tables_info:
    try:
        count = spark.table(table_name).count()
        print(f"✅ {table_name}: {count:,} records - {description}")
    except Exception as e:
        print(f"❌ {table_name}: Failed to create - {str(e)}")

print("="*50)
print("Pipeline execution completed!")

# COMMAND ----------

# Display sample data from the final joined table
print("\nSample data from silver_claim_policy table:")
display(spark.table("silver_claim_policy").limit(10))
