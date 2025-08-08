# Microsoft Fabric notebook source
# This notebook has been adapted from the original Databricks version
# Available at https://github.com/databricks-industry-solutions/smart-claims

# COMMAND ----------

# MAGIC %md
# MAGIC # Policy & Claims Data Ingestion Pipeline for Microsoft Fabric
# MAGIC * Tables:
# MAGIC   * bronze_claim & bronze_policy
# MAGIC   * silver_claim & silver_policy
# MAGIC   * silver_claim_policy (joined by policy id)
# MAGIC 
# MAGIC This pipeline uses the medallion architecture with Bronze, Silver, and Gold layers.

# COMMAND ----------

# Configure paths for Microsoft Fabric (OneLake or ADLS Gen2)
# Update these paths to match your Fabric workspace structure
lakehouse_name = "your_lakehouse_name"  # Replace with your actual lakehouse name

claims_path = f"abfss://your_workspace@onelake.dfs.fabric.microsoft.com/{lakehouse_name}/Files/data_sources/Claims"
policy_path = f"abfss://your_workspace@onelake.dfs.fabric.microsoft.com/{lakehouse_name}/Files/data_sources/Policy/policies.csv"
accident_path = f"abfss://your_workspace@onelake.dfs.fabric.microsoft.com/{lakehouse_name}/Files/data_sources/Accidents"

# Alternative: If using local lakehouse files
# claims_path = "Files/data_sources/Claims"
# policy_path = "Files/data_sources/Policy/policies.csv"
# accident_path = "Files/data_sources/Accidents"

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

# Set target schema for bronze layer
bronze = "bronze"
try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {bronze}")
    logger.info(f"Schema '{bronze}' is ready")
except Exception as e:
    logger.warning(f"Could not create schema {bronze}: {e}")

def write_delta_table(df, table_name, mode="overwrite", optimize=True):
    """
    Write DataFrame to Delta table within bronze schema (unless already qualified)
    """
    try:
        fq_name = table_name if "." in table_name else f"{bronze}.{table_name}"
        df.write \
          .format("delta") \
          .mode(mode) \
          .option("overwriteSchema", "true") \
          .saveAsTable(fq_name)
        if optimize:
            spark.sql(f"OPTIMIZE {fq_name}")
            logger.info(f"Table {fq_name} created and optimized successfully")
        return True
    except Exception as e:
        logger.error(f"Error writing table {table_name}: {str(e)}")
        return False


# COMMAND ----------

def create_bronze_claim():
    """
    Create bronze claims table from raw JSON files
    """
    try:
        logger.info("Creating bronze_claim table...")
        
        # Read JSON files
        df = spark.read.json(claims_path)
        
        # Add metadata columns
        df = df.withColumn("load_timestamp", F.current_timestamp()) \
               .withColumn("source_file", F.input_file_name())
        
        # Write to Delta table
        success = write_delta_table(df, "bronze_claim", mode="overwrite")
        
        if success:
            logger.info(f"Bronze claims table created with {df.count()} records")
            return df
        else:
            raise Exception("Failed to create bronze_claim table")
            
    except Exception as e:
        logger.error(f"Error creating bronze_claim: {str(e)}")
        raise

# Execute bronze claim creation
bronze_claim_df = create_bronze_claim()

# COMMAND ----------

def create_bronze_policy():
    """
    Create bronze policies table from CSV file
    """
    try:
        logger.info("Creating bronze_policy table...")
        
        # Read CSV file
        df = spark.read \
            .option("header", "true") \
            .option("sep", ",") \
            .option("inferSchema", "true") \
            .csv(policy_path)
        
        # Add metadata columns
        df = df.withColumn("load_timestamp", F.current_timestamp()) \
               .withColumn("source_file", F.lit(policy_path))
        
        # Write to Delta table
        success = write_delta_table(df, "bronze_policy", mode="overwrite")
        
        if success:
            logger.info(f"Bronze policy table created with {df.count()} records")
            return df
        else:
            raise Exception("Failed to create bronze_policy table")
            
    except Exception as e:
        logger.error(f"Error creating bronze_policy: {str(e)}")
        raise

# Execute bronze policy creation
bronze_policy_df = create_bronze_policy()


# COMMAND ----------

# Display summary of all created tables
print("="*50)
print("DATA PIPELINE EXECUTION SUMMARY")
print("="*50)

tables_info = [
    (f"{bronze}.bronze_claim", "Raw claims data from JSON files"),
    (f"{bronze}.bronze_policy", "Raw policy data from CSV file")
]

for table_name, description in tables_info:
    try:
        count = spark.table(table_name).count()
        print(f"✅ {table_name}: {count:,} records - {description}")
    except Exception as e:
        print(f"❌ {table_name}: Failed to create - {str(e)}")

print("="*50)
print("Pipeline execution completed!")
