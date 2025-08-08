# Microsoft Fabric notebook source
# This notebook has been adapted from the original Databricks version
# Available at https://github.com/databricks-industry-solutions/smart-claims

# COMMAND ----------

# Option 2: If data is in external storage (OneLake/ADLS Gen2)
lakehouse_name = "your_lakehouse_name"
workspace_name = "your_workspace_name"
telematics_path = f"abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/{lakehouse_name}/Files/data_sources/Telematics"

print(f"Telematics data path: {telematics_path}")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import types as T
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

print("✅ Libraries imported successfully")

# COMMAND ----------

# First, let's explore what parquet files are available
try:
    # For Fabric, we'll use a different approach to list files
    # Option 1: Try to read the directory directly
    print("Exploring telematics directory...")
    
    # Try to list files using Spark's file system utilities
    try:
        # Use Spark to list files in the directory
        file_df = spark.read.format("binaryFile").load(telematics_path)
        file_paths = [row.path for row in file_df.select("path").distinct().collect()]
        
        parquet_files = [path for path in file_paths if path.endswith('.parquet')]
        print(f"Found {len(parquet_files)} parquet files:")
        
        for i, file_path in enumerate(parquet_files[:10]):  # Show first 10 files
            file_name = file_path.split('/')[-1]
            print(f"{i+1}. {file_name}")
        
        if len(parquet_files) > 10:
            print(f"... and {len(parquet_files) - 10} more files")
            
    except Exception as inner_e:
        print(f"Could not list individual files: {str(inner_e)}")
        print("Will proceed to read all parquet files in the directory")
    
except Exception as e:
    print(f"Error exploring files: {str(e)}")
    print("Will attempt to read parquet files directly")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read and Examine Parquet Data

# COMMAND ----------

# Read a sample of the parquet data to understand the schema
print("Reading parquet files to examine schema and data...")

try:
    # Read all parquet files in the directory
    telematics_raw_df = spark.read.parquet(telematics_path)
    
    # Get basic information
    record_count = telematics_raw_df.count()
    column_count = len(telematics_raw_df.columns)
    
    print(f"📊 Data Overview:")
    print(f"Total records: {record_count:,}")
    print(f"Total columns: {column_count}")
    
    # Show schema
    print(f"\n📋 Schema:")
    telematics_raw_df.printSchema()
    
    # Show sample data
    print(f"\n📄 Sample Data:")
    display(telematics_raw_df.limit(5))
    
except Exception as e:
    logger.error(f"Error reading parquet files: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Assessment

# COMMAND ----------

# Perform data quality checks on the raw telematics data
print("Performing data quality assessment...")

try:
    # Check for null values in key columns
    total_records = telematics_raw_df.count()
    
    print(f"\n🔍 Data Quality Report:")
    print(f"Total records: {total_records:,}")
    
    # Analyze each column for nulls and data types
    for col_name in telematics_raw_df.columns:
        null_count = telematics_raw_df.filter(F.col(col_name).isNull()).count()
        null_percentage = (null_count / total_records) * 100 if total_records > 0 else 0
        
        print(f"  {col_name}: {null_count:,} nulls ({null_percentage:.1f}%)")
    
    # Check for duplicate records
    distinct_count = telematics_raw_df.distinct().count()
    duplicate_count = total_records - distinct_count
    
    print(f"\n📊 Duplicates:")
    print(f"Unique records: {distinct_count:,}")
    print(f"Duplicate records: {duplicate_count:,}")
    
    if duplicate_count > 0:
        print(f"Duplicate percentage: {(duplicate_count/total_records)*100:.1f}%")
        
except Exception as e:
    logger.error(f"Error in data quality assessment: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Transformation and Cleansing

# COMMAND ----------

# Apply transformations and cleansing to prepare for Delta table
print("Applying data transformations...")

try:
    # Start with the raw data
    telematics_cleaned_df = telematics_raw_df
    
    # Add metadata columns for tracking
    telematics_cleaned_df = (telematics_cleaned_df
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("ingestion_date", F.current_date())
        .withColumn("source_file", F.input_file_name())
    )
    
    # Remove exact duplicates if any
    initial_count = telematics_cleaned_df.count()
    telematics_cleaned_df = telematics_cleaned_df.distinct()
    final_count = telematics_cleaned_df.count()
    
    removed_duplicates = initial_count - final_count
    if removed_duplicates > 0:
        print(f"Removed {removed_duplicates:,} duplicate records")
    
    # Add row ID for tracking
    telematics_cleaned_df = telematics_cleaned_df.withColumn(
        "telematics_id", 
        F.monotonically_increasing_id()
    )
    
    print(f"✅ Data transformation completed")
    print(f"Final record count: {telematics_cleaned_df.count():,}")
    
    # Show sample of cleaned data
    print(f"\n📄 Sample of Cleaned Data:")
    display(telematics_cleaned_df.limit(5))
    
except Exception as e:
    logger.error(f"Error in data transformation: {str(e)}")
    raise

# COMMAND ----------

# Write the cleaned data as a Delta table
print("Creating silver_telematics Delta table...")

try:
    # Write to Delta table with partitioning for better performance
    # Assuming there might be date columns - adjust partitioning as needed
    
    print("Writing data to Delta table...")
    
    # Option 1: Simple write (use this if unsure about partitioning)
    (telematics_cleaned_df.write
     .format("delta")
     .mode("overwrite")  # Use "append" for incremental loads
     .option("overwriteSchema", "true")
     .saveAsTable("silver_telematics")
    )
    
    # Option 2: Write with partitioning (uncomment if you have date columns)
    # Adjust partition column based on your data
    # (telematics_cleaned_df.write
    #  .format("delta")
    #  .mode("overwrite")
    #  .option("overwriteSchema", "true")
    #  .partitionBy("ingestion_date")  # or another date column
    #  .saveAsTable("silver_telematics")
    # )
    
    print("✅ Delta table created successfully")
    
except Exception as e:
    logger.error(f"Error creating Delta table: {str(e)}")
    raise

# COMMAND ----------

# Optimize the Delta table for better query performance
print("Optimizing Delta table...")

try:
    # Basic optimization
    spark.sql("OPTIMIZE silver_telematics")
    print("✅ Table optimization completed")
    
    # Optional: Z-ORDER optimization for frequently queried columns
    # Uncomment and adjust column names based on your query patterns
    # spark.sql("OPTIMIZE silver_telematics ZORDER BY (column1, column2)")
    # print("✅ Z-ORDER optimization completed")
    
except Exception as e:
    logger.error(f"Error optimizing table: {str(e)}")

# COMMAND ----------

# Validate the created Delta table
print("Validating silver_telematics table...")

try:
    # Read the Delta table
    delta_table = spark.table("silver_telematics")
    
    # Get table statistics
    delta_count = delta_table.count()
    delta_columns = len(delta_table.columns)
    
    print(f"📊 Delta Table Validation:")
    print(f"Records in Delta table: {delta_count:,}")
    print(f"Columns in Delta table: {delta_columns}")
    
    # Show table schema
    print(f"\n📋 Delta Table Schema:")
    delta_table.printSchema()
    
    # Compare with original data
    original_count = telematics_raw_df.count()
    if delta_count == original_count:
        print(f"✅ Record count matches original data")
    else:
        print(f"⚠️  Record count difference: Original={original_count:,}, Delta={delta_count:,}")
    
    # Show sample data from Delta table
    print(f"\n📄 Sample from Delta Table:")
    display(delta_table.limit(5))
    
    # Show table properties
    print(f"\n🔧 Table Information:")
    spark.sql("DESCRIBE EXTENDED silver_telematics").show(50, False)
    
except Exception as e:
    logger.error(f"Error validating Delta table: {str(e)}")

# COMMAND ----------

print("="*60)
print("TELEMATICS DATA INGESTION SUMMARY")
print("="*60)

try:
    # Final statistics
    telematics_table = spark.table("silver_telematics")
    final_record_count = telematics_table.count()
    
    print(f"✅ Successfully created silver_telematics Delta table")
    print(f"📊 Final record count: {final_record_count:,}")
    print(f"📁 Source: Parquet files from {telematics_path}")
    print(f"🗃️  Destination: silver_telematics Delta table")
    
    # File information - simplified for Fabric
    print(f"📄 Processed parquet files from telematics directory")
    
    print(f"\n🎯 Next Steps:")
    print(f"1. ✅ silver_telematics table is ready for analytics")
    print(f"2. 🔗 Join with claims/policy data for insights")
    print(f"3. 📈 Create Power BI reports using this data")
    print(f"4. 🔄 Set up incremental processing for new data")
    print(f"5. 📊 Consider adding data quality monitoring")
    
except Exception as e:
    print(f"❌ Error in final summary: {str(e)}")

print("="*60)
print("✅ Telematics data ingestion completed!")
print("="*60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional: Data Profiling

# COMMAND ----------

# Optional: Perform detailed data profiling
print("Performing detailed data profiling...")

try:
    telematics_table = spark.table("silver_telematics")
    
    # Basic statistics for numeric columns
    numeric_columns = [f.name for f in telematics_table.schema.fields 
                      if isinstance(f.dataType, (T.IntegerType, T.LongType, T.FloatType, T.DoubleType))]
    
    if numeric_columns:
        print(f"\n📊 Numeric Column Statistics:")
        stats_df = telematics_table.select(numeric_columns).describe()
        display(stats_df)
    
    # Count distinct values for categorical columns (limit to reasonable number)
    categorical_columns = [f.name for f in telematics_table.schema.fields 
                          if isinstance(f.dataType, T.StringType)][:5]  # Limit to first 5 string columns
    
    if categorical_columns:
        print(f"\n📊 Categorical Column Distinct Counts:")
        for col in categorical_columns:
            distinct_count = telematics_table.select(col).distinct().count()
            print(f"  {col}: {distinct_count:,} distinct values")
    
except Exception as e:
    logger.error(f"Error in data profiling: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Monitoring Setup

# COMMAND ----------

# Set up basic data quality monitoring
print("Setting up data quality monitoring...")

try:
    telematics_table = spark.table("silver_telematics")
    
    # Create a simple data quality report
    total_records = telematics_table.count()
    
    quality_report = {
        "table_name": "silver_telematics",
        "total_records": total_records,
        "ingestion_timestamp": telematics_table.select(F.max("ingestion_timestamp")).collect()[0][0],
        "null_counts": {},
        "data_completeness": {}
    }
    
    # Calculate null percentages for each column
    for col_name in telematics_table.columns:
        if col_name not in ["telematics_id", "ingestion_timestamp", "ingestion_date", "source_file"]:
            null_count = telematics_table.filter(F.col(col_name).isNull()).count()
            null_percentage = (null_count / total_records) * 100 if total_records > 0 else 0
            
            quality_report["null_counts"][col_name] = null_count
            quality_report["data_completeness"][col_name] = f"{100 - null_percentage:.1f}%"
    
    print(f"\n📋 Data Quality Report:")
    print(f"Table: {quality_report['table_name']}")
    print(f"Total Records: {quality_report['total_records']:,}")
    print(f"Last Updated: {quality_report['ingestion_timestamp']}")
    
    print(f"\n📊 Data Completeness by Column:")
    for col, completeness in quality_report["data_completeness"].items():
        print(f"  {col}: {completeness}")
    
    # You could save this report to a monitoring table for tracking over time
    # quality_df = spark.createDataFrame([quality_report])
    # quality_df.write.format("delta").mode("append").saveAsTable("data_quality_reports")
    
except Exception as e:
    logger.error(f"Error setting up data quality monitoring: {str(e)}")

print("\n✅ Data quality monitoring setup completed")
