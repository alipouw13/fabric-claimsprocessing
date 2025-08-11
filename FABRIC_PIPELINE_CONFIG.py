# Microsoft Fabric Data Pipeline Configuration
# Smart Claims Processing Pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC # Smart Claims Data Pipeline - Fabric Configuration
# MAGIC 
# MAGIC This notebook creates and configures the Microsoft Fabric Data Pipeline equivalent to the Databricks workflow.
# MAGIC 
# MAGIC **Pipeline Architecture:**
# MAGIC - 📊 **Sequential Processing**: Core data ingestion first
# MAGIC - 🔄 **Parallel Branches**: Independent processing streams  
# MAGIC - 🎯 **Final Integration**: Combine all data for rules engine
# MAGIC - 📈 **Analytics Ready**: Prepare data for Power BI

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Configuration

# COMMAND ----------

# Fabric pipeline configuration equivalent to Databricks job_json
fabric_pipeline_config = {
    "pipeline_name": "Smart Claims Processing Pipeline",
    "description": "End-to-end claims processing with AI-powered insights",
    "timeout_minutes": 480,  # 8 hours
    "max_concurrent_activities": 3,
    "tags": {
        "usage": "production",
        "domain": "insurance", 
        "solution": "smart_claims"
    },
    "schedule": {
        "frequency": "Daily",
        "start_time": "02:00:00",
        "timezone": "UTC",
        "enabled": False  # Set to True when ready for production
    },
    "parameters": {
        "lakehouse_name": "smart-claims-lakehouse",
        "workspace_name": "your-workspace-name",
        "environment": "production",
        "debug_mode": False,
        "data_retention_days": 90
    }
}

print("📋 Fabric Pipeline Configuration:")
print(f"   Name: {fabric_pipeline_config['pipeline_name']}")
print(f"   Timeout: {fabric_pipeline_config['timeout_minutes']} minutes")
print(f"   Lakehouse: {fabric_pipeline_config['parameters']['lakehouse_name']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Activity Definitions

# COMMAND ----------

# Updated pipeline activities to reflect current notebook set (bronze → silver → enrichment → ML → rules → gold)
pipeline_activities = [
    {
        "activity_name": "00_setup_validation",
        "type": "Notebook",
        "description": "Initial setup and environment/schema validation",
        "notebook_path": "00_README",  # optional overview / validation
        "timeout_minutes": 15,
        "retry_count": 1,
        "depends_on": [],
        "parameters": {"create_schemas": True, "validation_mode": "light"}
    },
    {
        "activity_name": "01_source_to_bronze",
        "type": "Notebook",
        "description": "Ingest raw policy & claims to bronze schema",
        "notebook_path": "01_policy_claims_sourceToBronze",
        "timeout_minutes": 40,
        "retry_count": 2,
        "depends_on": ["00_setup_validation"],
        "parameters": {"mode": "overwrite", "data_quality_checks": True}
    },
    {
        "activity_name": "02_bronze_to_silver",
        "type": "Notebook",
        "description": "Normalize & conform policy / claim data to silver schema",
        "notebook_path": "02_policy_claims_bronzeToSilver",
        "timeout_minutes": 45,
        "retry_count": 2,
        "depends_on": ["01_source_to_bronze"],
        "parameters": {"optimize": True}
    },
    {
        "activity_name": "03_iot_telematics",
        "type": "Notebook",
        "description": "Ingest & cleanse telematics parquet to bronze/silver",
        "notebook_path": "03_iot",
        "timeout_minutes": 45,
        "retry_count": 2,
        "depends_on": ["02_bronze_to_silver"],
        "parameters": {"partition": "ingestion_date"}
    },
    {
        "activity_name": "04_location_enrichment",
        "type": "Notebook",
        "description": "Geo enrichment (zipcode → lat/long) for claim/policy join",
        "notebook_path": "04_policy_location",
        "timeout_minutes": 50,
        "retry_count": 2,
        "depends_on": ["02_bronze_to_silver"],
        "parameters": {"batch_size": 100, "rate_limit_delay": 1.0}
    },
    {
        "activity_name": "05a_images_source_to_bronze",
        "type": "Notebook",
        "description": "Accident image metadata & binary ingestion to bronze",
        "notebook_path": "05a_accident_images_sourceToBronze",
        "timeout_minutes": 60,
        "retry_count": 2,
        "depends_on": ["01_source_to_bronze"],
        "parameters": {"optimize_after_write": True}
    },
    {
        "activity_name": "05_import_model",
        "type": "Notebook",
        "description": "Import/prepare ML model artifacts for severity scoring",
        "notebook_path": "05_import_model",
        "timeout_minutes": 20,
        "retry_count": 1,
        "depends_on": ["01_source_to_bronze"],
        "parameters": {"register_if_missing": True}
    },
    {
        "activity_name": "05b_severity_prediction",
        "type": "Notebook",
        "description": "Incremental ML severity scoring producing silver_accident",
        "notebook_path": "05b_severity_prediction_bronzeToSilver",
        "timeout_minutes": 90,
        "retry_count": 2,
        "depends_on": ["05a_images_source_to_bronze", "05_import_model"],
        "parameters": {"force_incremental": True, "include_content": False}
    },
    {
        "activity_name": "06_rules_engine",
        "type": "Notebook",
        "description": "Apply business rules and persist gold.gold_insights",
        "notebook_path": "06_rules_engine",
        "timeout_minutes": 35,
        "retry_count": 2,
        "depends_on": ["04_location_enrichment", "05b_severity_prediction", "03_iot_telematics"],
        "parameters": {"rules_version": "latest", "audit_logging": True}
    },
    {
        "activity_name": "07_gold_views_materialization",
        "type": "Notebook",
        "description": "Create gold star-schema dimensions, facts, and reporting views",
        "notebook_path": "07_policy_claims_accident_Goldviews",  # Python notebook
        "timeout_minutes": 30,
        "retry_count": 1,
        "depends_on": ["06_rules_engine"],
        "parameters": {"refresh_mode": "full"}
    }
]

print("🔁 Updated Pipeline Activities (new notebook set):")
for act in pipeline_activities:
    print(f"   - {act['activity_name']} (depends: {len(act['depends_on'])})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Path Configuration

# COMMAND ----------

# Data source paths in Fabric Lakehouse
data_paths = {
    "base_path": f"abfss://{fabric_pipeline_config['parameters']['workspace_name']}@onelake.dfs.fabric.microsoft.com/{fabric_pipeline_config['parameters']['lakehouse_name']}",
    "source_data": {
        "claims": "Files/data_sources/Claims/",
        "policies": "Files/data_sources/Policies/policies.csv", 
        "accidents": "Files/data_sources/Accidents/",
        "telematics": "Files/data_sources/Telematics/",
        "metadata": "Files/data_sources/Accidents/image_metadata.csv"
    },
    "delta_tables": {
        "bronze_claim": "Tables/bronze/bronze_claim",
        "bronze_policy": "Tables/bronze/bronze_policy",
        "bronze_accident": "Tables/bronze/bronze_accident",
        "bronze_images": "Tables/bronze/bronze_images",
        "silver_claim": "Tables/silver/silver_claim",
        "silver_policy": "Tables/silver/silver_policy",
        "silver_claim_policy": "Tables/silver/silver_claim_policy",
        "silver_claim_policy_location": "Tables/silver/silver_claim_policy_location",
        "silver_telematics": "Tables/silver/silver_telematics",
        "silver_accident": "Tables/silver/silver_accident",
        "silver_claim_policy_accident": "Tables/silver/silver_claim_policy_accident",
        "gold_insights": "Tables/gold/gold_insights"
    },
    "checkpoints": "Files/pipeline_checkpoints/",
    "logs": "Files/pipeline_logs/"
}

print("📁 Data Path Configuration:")
print(f"   Base Path: {data_paths['base_path']}")
print(f"   Source Tables: {len(data_paths['source_data'])}")
print(f"   Delta Tables: {len(data_paths['delta_tables'])}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Environment Configuration

# COMMAND ----------

# Environment-specific settings
environment_config = {
    "development": {
        "spark_config": {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true", 
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.databricks.delta.autoCompact.enabled": "true"
        },
        "resource_allocation": {
            "driver_memory": "8g",
            "executor_memory": "8g", 
            "max_executors": 4
        },
        "debug_settings": {
            "verbose_logging": True,
            "sample_data_only": True,
            "validation_checks": "strict"
        }
    },
    "production": {
        "spark_config": {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true", 
            "spark.databricks.delta.autoCompact.enabled": "true",
            "spark.sql.adaptive.advisoryPartitionSizeInBytes": "268435456"  # 256MB
        },
        "resource_allocation": {
            "driver_memory": "16g",
            "executor_memory": "16g",
            "max_executors": 10
        },
        "debug_settings": {
            "verbose_logging": False,
            "sample_data_only": False,
            "validation_checks": "standard"
        }
    }
}

current_env = fabric_pipeline_config['parameters']['environment']
active_config = environment_config[current_env]

print(f"🔧 Environment Configuration: {current_env}")
print(f"   Max Executors: {active_config['resource_allocation']['max_executors']}")
print(f"   Validation Level: {active_config['debug_settings']['validation_checks']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitoring Configuration

# COMMAND ----------

# Monitoring and alerting configuration
monitoring_config = {
    "metrics": {
        "data_quality": {
            "null_threshold": 0.1,  # 10% null rate threshold
            "schema_drift_detection": True,
            "row_count_validation": True
        },
        "performance": {
            "execution_time_threshold_minutes": 120,
            "memory_usage_threshold": 0.8,
            "cpu_usage_threshold": 0.75
        },
        "business": {
            "daily_claims_volume": {"min": 100, "max": 10000},
            "severity_score_range": {"min": 0.0, "max": 1.0},
            "geocoding_success_rate": 0.85
        }
    },
    "alerts": {
        "email_recipients": [
            "data-team@company.com",
            "business-team@company.com"
        ],
        "teams_webhook": "https://company.webhook.office.com/webhookb2/...",
        "alert_levels": {
            "warning": ["data_quality_degradation", "performance_slow"],
            "critical": ["pipeline_failure", "data_corruption"]
        }
    },
    "dashboards": {
        "pipeline_health": "Smart Claims - Pipeline Health",
        "data_quality": "Smart Claims - Data Quality", 
        "business_metrics": "Smart Claims - Business KPIs"
    }
}

print("📊 Monitoring Configuration:")
print(f"   Quality Thresholds: {len(monitoring_config['metrics']['data_quality'])}")
print(f"   Alert Recipients: {len(monitoring_config['alerts']['email_recipients'])}")
print(f"   Dashboards: {len(monitoring_config['dashboards'])}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Deployment Functions

# COMMAND ----------

def create_fabric_pipeline():
    """
    Create the Microsoft Fabric Data Pipeline
    This function demonstrates the pipeline creation process
    """
    print("🚀 Creating Microsoft Fabric Data Pipeline...")
    
    # In actual implementation, you would use Fabric REST APIs or SDKs
    pipeline_definition = {
        "name": fabric_pipeline_config["pipeline_name"],
        "description": fabric_pipeline_config["description"],
        "activities": []
    }
    
    # Convert activities to Fabric pipeline format
    for activity in pipeline_activities:
        fabric_activity = {
            "name": activity["activity_name"],
            "type": "ExecuteNotebook" if activity["type"] == "Notebook" else activity["type"],
            "typeProperties": {
                "notebook": {
                    "referenceName": activity["notebook_path"],
                    "type": "NotebookReference"
                },
                "parameters": activity["parameters"]
            },
            "dependsOn": [
                {
                    "activity": dep,
                    "dependencyConditions": ["Succeeded"]
                } for dep in activity["depends_on"]
            ]
        }
        pipeline_definition["activities"].append(fabric_activity)
    
    print(f"✅ Pipeline definition created with {len(pipeline_definition['activities'])} activities")
    return pipeline_definition

def validate_prerequisites():
    """
    Validate that all prerequisites are met before deployment
    """
    print("🔍 Validating prerequisites...")
    
    checks = {
        "lakehouse_exists": False,  # Would check if lakehouse exists
        "notebooks_imported": False,  # Would check if notebooks are available
        "data_uploaded": False,  # Would check if source data is available
        "permissions_valid": False  # Would check user permissions
    }
    
    # Simulate validation checks
    for check, status in checks.items():
        print(f"   {check}: {'✅ Pass' if status else '⚠️ Needs attention'}")
    
    return all(checks.values())

def deploy_pipeline():
    """
    Deploy the complete Smart Claims pipeline to Fabric
    """
    print("🎯 Starting pipeline deployment...")
    
    # Step 1: Validate prerequisites
    if not validate_prerequisites():
        print("❌ Prerequisites not met. Please address the issues above.")
        return False
    
    # Step 2: Create pipeline definition
    pipeline_def = create_fabric_pipeline()
    
    # Step 3: Deploy to Fabric (simulated)
    print("📤 Deploying to Microsoft Fabric...")
    print("   • Creating pipeline...")
    print("   • Configuring activities...")
    print("   • Setting up dependencies...")
    print("   • Applying security settings...")
    
    # Step 4: Configure monitoring
    print("📊 Setting up monitoring...")
    print("   • Creating data quality alerts...")
    print("   • Configuring performance monitoring...")
    print("   • Setting up business metric tracking...")
    
    print("✅ Pipeline deployment completed!")
    print(f"📋 Pipeline Name: {fabric_pipeline_config['pipeline_name']}")
    print("🔗 Next steps:")
    print("   1. Import the Fabric notebooks")
    print("   2. Upload source data to lakehouse")
    print("   3. Test the pipeline with sample data")
    print("   4. Configure Power BI reports")
    
    return True

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execution Summary

# COMMAND ----------

print("="*60)
print("FABRIC PIPELINE CONFIGURATION SUMMARY")
print("="*60)

print(f"📊 Pipeline Overview:")
print(f"   Name: {fabric_pipeline_config['pipeline_name']}")
print(f"   Activities: {len(pipeline_activities)}")
print(f"   Environment: {fabric_pipeline_config['parameters']['environment']}")
print(f"   Timeout: {fabric_pipeline_config['timeout_minutes']} minutes")

print(f"\n🗂️ Data Sources:")
for source, path in data_paths['source_data'].items():
    print(f"   {source}: {path}")

print(f"\n📊 Delta Tables:")
for table, path in data_paths['delta_tables'].items():
    print(f"   {table}: {path}")

print(f"\n🔧 Key Features:")
print(f"   • Parallel processing branches")
print(f"   • Comprehensive error handling") 
print(f"   • Data quality monitoring")
print(f"   • Automated retry logic")
print(f"   • Performance optimization")

print(f"\n📈 Business Value:")
print(f"   • Automated claims processing")
print(f"   • AI-powered damage assessment")
print(f"   • Location-based insights")
print(f"   • Dynamic business rules")
print(f"   • Real-time analytics")

print(f"\n🎯 Deployment Steps:")
print(f"   1. ✅ Configuration defined")
print(f"   2. 📋 Prerequisites validation")
print(f"   3. 🚀 Pipeline creation") 
print(f"   4. 📊 Monitoring setup")
print(f"   5. 🔄 Testing and validation")

print("="*60)
print("Ready for Microsoft Fabric deployment! 🚀")
print("="*60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manual Deployment Instructions

# COMMAND ----------

print("""
🔧 MANUAL DEPLOYMENT GUIDE

To deploy this pipeline in Microsoft Fabric:

1. **Create Data Pipeline**
   • Go to your Fabric workspace
   • Click '+ New' → 'Data Pipeline'
   • Name it 'Smart Claims Processing Pipeline'

2. **Add Notebook Activities**
   For each notebook in pipeline_activities:
   • Drag 'Notebook' activity to canvas
   • Configure notebook reference
   • Set parameters as specified
   • Configure dependencies

3. **Configure Scheduling**
   • Set schedule to run daily at 2:00 AM UTC
   • Enable retry on failure (3 attempts)
   • Set timeout to 8 hours

4. **Set Up Monitoring**
   • Configure email alerts for failures
   • Set up performance monitoring
   • Create data quality checks

5. **Test Pipeline**
   • Run with sample data first
   • Validate all activities complete successfully
   • Check output tables are created correctly

6. **Production Deployment**
   • Update with production data paths
   • Configure production schedule
   • Enable monitoring alerts
   • Document operational procedures

For detailed instructions, see: FABRIC_DEPLOYMENT_GUIDE.md
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration Export

# COMMAND ----------

# Export configuration for documentation
import json

export_config = {
    "pipeline_config": fabric_pipeline_config,
    "activities": pipeline_activities,
    "data_paths": data_paths,
    "environment": environment_config,
    "monitoring": monitoring_config
}

# Would save to file in actual implementation
print("📤 Configuration export ready")
print(f"   Total activities: {len(pipeline_activities)}")
print(f"   Configuration size: {len(str(export_config))} characters")
print("   Use this configuration to replicate the pipeline setup")

# Display formatted JSON (first 1000 characters)
config_json = json.dumps(export_config, indent=2)
print(f"\n📋 Configuration Preview:")
print(config_json[:1000] + "..." if len(config_json) > 1000 else config_json)
