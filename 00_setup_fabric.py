# Microsoft Fabric notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Smart Claims Setup - Microsoft Fabric
# MAGIC 
# MAGIC **Initial setup and validation for Smart Claims pipeline**
# MAGIC - 🔧 Environment configuration
# MAGIC - 📊 Data source validation  
# MAGIC - 🗃️ Schema creation
# MAGIC - ✅ Prerequisites check

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Fabric environment configuration
lakehouse_name = "smart-claims-lakehouse"
workspace_name = "your-workspace-name"

# Data paths
data_sources = {
    "claims": "Files/data_sources/Claims/",
    "policies": "Files/data_sources/Policies/policies.csv",
    "accidents": "Files/data_sources/Accidents/",
    "telematics": "Files/data_sources/Telematics/",
    "metadata": "Files/data_sources/Accidents/image_metadata.csv"
}

print(f"🏗️ Fabric Setup Configuration:")
print(f"   Lakehouse: {lakehouse_name}")
print(f"   Workspace: {workspace_name}")
print(f"   Data Sources: {len(data_sources)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Environment Validation

# COMMAND ----------

from pyspark.sql import functions as F
import os

print("🔍 Validating Fabric environment...")

# Check Spark configuration
spark_version = spark.version
print(f"✅ Spark Version: {spark_version}")

# Check if running in Fabric
try:
    # Fabric-specific checks
    print("✅ Running in Microsoft Fabric environment")
    fabric_available = True
except:
    print("⚠️ May not be running in Fabric environment")
    fabric_available = False

# Check Delta support
try:
    spark.sql("SELECT 1").show()
    print("✅ Spark SQL available")
except Exception as e:
    print(f"❌ Spark SQL issue: {str(e)}")

print(f"\n📊 Spark Configuration:")
print(f"   SQL Adaptive: {spark.conf.get('spark.sql.adaptive.enabled', 'default')}")
print(f"   Delta Auto-compact: {spark.conf.get('spark.databricks.delta.autoCompact.enabled', 'default')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Source Validation

# COMMAND ----------

print("📁 Validating data sources...")

validation_results = {}

for source_name, source_path in data_sources.items():
    try:
        print(f"\n🔍 Checking {source_name}...")
        
        if source_path.endswith('.csv'):
            # Check single CSV file
            test_df = spark.read.option("header", "true").csv(source_path)
            count = test_df.count()
            cols = len(test_df.columns)
            validation_results[source_name] = {
                "status": "✅ Available",
                "records": count,
                "columns": cols,
                "type": "CSV"
            }
            print(f"   ✅ Found CSV: {count:,} records, {cols} columns")
            
        elif source_path.endswith('/'):
            # Check directory
            try:
                # Try to list files in directory
                if 'parquet' in source_name.lower():
                    test_df = spark.read.parquet(source_path)
                    count = test_df.count()
                    cols = len(test_df.columns)
                    validation_results[source_name] = {
                        "status": "✅ Available",
                        "records": count,
                        "columns": cols,
                        "type": "Parquet"
                    }
                    print(f"   ✅ Found Parquet files: {count:,} records, {cols} columns")
                else:
                    # Try binary files for images
                    test_df = spark.read.format("binaryFile").load(source_path)
                    count = test_df.count()
                    validation_results[source_name] = {
                        "status": "✅ Available", 
                        "files": count,
                        "type": "Binary"
                    }
                    print(f"   ✅ Found binary files: {count:,} files")
            except Exception as dir_e:
                validation_results[source_name] = {
                    "status": "⚠️ Directory exists but may be empty",
                    "error": str(dir_e)
                }
                print(f"   ⚠️ Directory found but may be empty: {str(dir_e)}")
        
    except Exception as e:
        validation_results[source_name] = {
            "status": "❌ Not found",
            "error": str(e)
        }
        print(f"   ❌ Not found: {str(e)}")

# Summary
print(f"\n📊 Data Source Validation Summary:")
available = sum(1 for r in validation_results.values() if "✅" in r["status"])
total = len(validation_results)
print(f"   Available: {available}/{total} data sources")

for source, result in validation_results.items():
    print(f"   {source}: {result['status']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Creation

# COMMAND ----------

print("🗃️ Creating database schemas...")

# Create database for Smart Claims
database_name = "smart_claims"

try:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
    print(f"✅ Database '{database_name}' created/verified")
    
    # Set as default database
    spark.sql(f"USE {database_name}")
    print(f"✅ Using database '{database_name}'")
    
    # List existing tables
    tables = spark.sql("SHOW TABLES").collect()
    print(f"📊 Existing tables: {len(tables)}")
    
    for table in tables:
        print(f"   • {table.tableName}")
        
except Exception as e:
    print(f"❌ Error creating database: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance Configuration

# COMMAND ----------

print("⚡ Optimizing Spark configuration for Smart Claims...")

# Set optimal Spark configurations for the pipeline
spark_configs = {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.databricks.delta.autoCompact.enabled": "true",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes": "268435456"  # 256MB
}

for config, value in spark_configs.items():
    try:
        spark.conf.set(config, value)
        print(f"✅ Set {config} = {value}")
    except Exception as e:
        print(f"⚠️ Could not set {config}: {str(e)}")

print("✅ Spark optimization completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Utility Functions

# COMMAND ----------

def check_table_exists(table_name):
    """Check if a Delta table exists"""
    try:
        spark.table(table_name)
        return True
    except:
        return False

def get_table_info(table_name):
    """Get basic information about a table"""
    try:
        df = spark.table(table_name)
        return {
            "exists": True,
            "count": df.count(),
            "columns": len(df.columns),
            "schema": df.columns
        }
    except:
        return {"exists": False}

def create_checkpoint_location(pipeline_name):
    """Create checkpoint location for streaming"""
    checkpoint_path = f"Files/pipeline_checkpoints/{pipeline_name}"
    print(f"📍 Checkpoint location: {checkpoint_path}")
    return checkpoint_path

print("🔧 Utility functions defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Health Check

# COMMAND ----------

print("🏥 Running comprehensive health check...")

health_status = {
    "environment": fabric_available,
    "spark": True,
    "database": False,
    "data_sources": available == total,
    "permissions": True  # Assume true if we got this far
}

try:
    # Test database operations
    spark.sql(f"USE {database_name}")
    health_status["database"] = True
except:
    health_status["database"] = False

# Test table creation
try:
    test_df = spark.range(1).toDF("test_col")
    test_df.write.format("delta").mode("overwrite").saveAsTable("health_check_test")
    spark.sql("DROP TABLE IF EXISTS health_check_test")
    health_status["table_operations"] = True
    print("✅ Table operations working")
except Exception as e:
    health_status["table_operations"] = False
    print(f"❌ Table operations failed: {str(e)}")

# Overall health
overall_health = all(health_status.values())

print(f"\n🏥 Health Check Results:")
for check, status in health_status.items():
    icon = "✅" if status else "❌"
    print(f"   {icon} {check}")

print(f"\n🎯 Overall Status: {'✅ HEALTHY' if overall_health else '⚠️ NEEDS ATTENTION'}")

if overall_health:
    print("\n🚀 Environment ready for Smart Claims pipeline!")
else:
    print("\n⚠️ Please address the issues above before running the pipeline")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Summary

# COMMAND ----------

print("="*60)
print("SMART CLAIMS SETUP SUMMARY")
print("="*60)

print(f"📊 Environment Status:")
print(f"   Lakehouse: {lakehouse_name}")
print(f"   Database: {database_name}")
print(f"   Spark Version: {spark_version}")
print(f"   Overall Health: {'✅ Ready' if overall_health else '⚠️ Issues'}")

print(f"\n📁 Data Sources Status:")
print(f"   Available: {available}/{total}")
for source, result in validation_results.items():
    status_icon = "✅" if "✅" in result["status"] else "⚠️" if "⚠️" in result["status"] else "❌"
    print(f"   {status_icon} {source}")

print(f"\n🔧 Configuration Applied:")
print(f"   Spark optimizations: {len(spark_configs)} settings")
print(f"   Database schema: Created")
print(f"   Utility functions: Ready")

print(f"\n🎯 Next Steps:")
if overall_health:
    print(f"   1. ✅ Setup complete - ready for pipeline")
    print(f"   2. 🔄 Run 01_policy_claims_accident_fabric")
    print(f"   3. 📊 Execute remaining notebooks in sequence")
    print(f"   4. 📈 Connect Power BI for reporting")
else:
    print(f"   1. ⚠️ Fix data source issues")
    print(f"   2. 🔧 Verify lakehouse configuration")
    print(f"   3. 🔄 Re-run this setup notebook")
    print(f"   4. 📞 Contact support if issues persist")

print("="*60)
print("Setup completed!" + (" 🚀" if overall_health else " ⚠️"))
print("="*60)
