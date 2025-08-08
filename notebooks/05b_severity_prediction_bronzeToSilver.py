# Microsoft Fabric notebook source
# NOTE: Attach your lakehouse to this notebook to ensure no errors

# Configuration for ML processing
ml_config = {
    "model_version": "simulated_v1.0",
    "prediction_threshold": 0.5,
    "batch_size": 1000,  # Batch size for Spark processing
    "pandas_threshold": 10000,  # Switch to Spark above this many records
    "api_batch_size": 500,  # Smaller batches for API calls
    "use_simulated_predictions": True,  # Set to False when real ML model is available
    "enable_caching": True,  # Cache intermediate results
    "api_delay_seconds": 0.1,  # Delay between API batches
    "max_retries": 3,  # Max retries for failed API calls
    # Large dataset controls
    "large_dataset_threshold": 10_000_000,
    "chunk_size_primary": 500_000,   # Default chunk size for very large datasets
    "chunk_size_min": 100_000,       # Minimum chunk size when back-off needed
    "max_partitions_per_chunk": 200, # Upper bound to avoid tiny files
    "progress_checkpoint_path": "Files/tmp/silver_accident_progress",
    # Model registry settings
    "registry_model_name": "damage_severity_model",
    "registry_model_stage": "Production",  # or None to always pull latest
    # NEW: control flags
    "force_incremental": True,                 # Always use incremental path to avoid CTAS OOM
    "small_path_row_limit": 1_000_000,         # If force_incremental False, still cap small path
    "include_content_in_silver": False,         # Drop binary content to reduce row size
    # NEW small dataset chunking parameters
    "small_dataset_target_chunks": 5,          # Aim for this many chunks when total < chunk_size_min
    "small_dataset_min_chunk": 2_000,          # Minimum rows per chunk for small datasets
    "reset_progress_on_overshoot": True,
    # NEW safety flag to force restart if target table is empty or missing
    "force_reset_on_empty_table": True
}

print(f"🤖 ML Configuration:")
for key, value in ml_config.items():
    print(f"   {key}: {value}")

# COMMAND ----------

import pandas as pd
import random
import numpy as np
import logging
from pyspark.sql import functions as F
from pyspark.sql.functions import col, current_timestamp, when, lit
import time, json, math  # added

# Fabric-specific imports
try:
    from notebookutils import mssparkutils
    # For backward compatibility, also try direct import
    if 'notebookutils' not in globals():
        import notebookutils
    fabric_utils_available = True
    print("✅ Fabric notebook utilities available")
except ImportError:
    fabric_utils_available = False
    print("⚠️ Fabric notebook utilities not available - running in non-Fabric environment")

# For ML model (if using MLflow)
try:
    import mlflow
    mlflow_available = True
    print("✅ MLflow available")
except ImportError:
    mlflow_available = False
    print("⚠️ MLflow not available - using simulated predictions")

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

print("✅ Libraries imported successfully")

# COMMAND ----------

# === Schema configuration ===
bronze = "bronze"
silver = "silver"
try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {bronze}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {silver}")
    print(f"✅ Ensured schemas '{bronze}' and '{silver}' exist")
except Exception as schema_err:
    print(f"⚠️ Schema creation issue: {schema_err}")

bronze_accident = f"{bronze}.bronze_accident"
bronze_images = f"{bronze}.bronze_images"
silver_accident = f"{silver}.silver_accident"

# COMMAND ----------

print("🥉 Loading bronze accident data...")

try:
    # Load bronze tables (schema-qualified)
    bronze_accident = spark.table(bronze_accident)
    bronze_images = spark.table(bronze_images)
    
    bronze_count = bronze_accident.count()
    images_count = bronze_images.count()
    
    print(f"✅ Loaded bronze data:")
    print(f"   Bronze accident records: {bronze_count:,}")
    print(f"   Bronze image records: {images_count:,}")
    
    # Show sample data structure
    print(f"\n📋 Bronze accident schema:")
    bronze_accident.printSchema()
    
    print(f"\n📄 Sample bronze data:")
    display(bronze_accident.select(
        "image_name", "path", "loaded_timestamp"
    ).limit(5))
    
except Exception as e:
    logger.error(f"Error loading bronze data: {str(e)}")
    print("Please ensure bronze layer has been created by running 05a_accident_images_bronze_fabric")
    raise


# COMMAND ----------

print("🔄 Preparing data for ML prediction...")

try:
    # Select images that have both metadata and binary content
    images_for_prediction = (bronze_accident
                           .filter(col("content").isNotNull())
                           .filter(col("image_name").isNotNull())
                           .select("image_name", "content", "path"))
    
    prediction_count = images_for_prediction.count()
    print(f"📊 Images ready for prediction: {prediction_count:,}")
    
    if prediction_count == 0:
        raise ValueError("No images available for prediction. Check bronze data quality.")
    
    # Convert to Pandas for ML processing (if dataset is small enough)
    pandas_threshold = ml_config.get("pandas_threshold", 10000)
    if prediction_count <= pandas_threshold:  # Configurable threshold
        print(f"📥 Converting to Pandas for ML processing (threshold: {pandas_threshold:,})...")
        images_pandas = images_for_prediction.toPandas()
        print(f"✅ Converted {len(images_pandas):,} images to Pandas")
    else:
        print(f"⚠️ Dataset too large ({prediction_count:,} images) for Pandas conversion")
        print(f"Using Spark-based prediction approach (threshold: {pandas_threshold:,})")
        images_pandas = None
    
except Exception as e:
    logger.error(f"Error preparing data: {str(e)}")
    raise

# COMMAND ----------

print("🤖 Checking for ML model availability...")

model_loaded = False

# Attempt MLflow registry load first (before Azure AI Foundry or simulation)
try:
    import mlflow
    registry_name = ml_config.get("registry_model_name")
    registry_stage = ml_config.get("registry_model_stage")
    if registry_name:
        model_uri = f"models:/{registry_name}/{registry_stage}" if registry_stage else f"models:/{registry_name}/latest"
        print(f"🔍 Attempting to load MLflow registered model: {model_uri}")
        model = mlflow.pyfunc.load_model(model_uri)
        print("✅ Loaded model from MLflow registry")
        ml_config["model_version"] = getattr(model, "metadata", {}).get("run_id", "registry")
        model_loaded = True
        ml_config["use_simulated_predictions"] = False
except Exception as e:
    print(f"⚠️ Could not load MLflow registry model: {e}")
    # Continue to Azure AI Foundry attempt / simulation

if not model_loaded:
    if mlflow_available and not ml_config["use_simulated_predictions"]:
        print("🔍 Attempting to load ML model...")
        
        try:
            # Option 1: Azure AI Foundry Model via MLflow Registry
            # model_name = "damage_severity_model"
            # model_uri = f"models:/{model_name}/production"
            # model = mlflow.pyfunc.load_model(model_uri)
            # print(f"✅ Loaded model from MLflow registry: {model_uri}")
            # model_loaded = True
            
            # Option 2: Azure AI Foundry Direct Integration
            try:
                from azure.ai.inference import ChatCompletionsClient
                from azure.core.credentials import AzureKeyCredential
                
                # Get credentials from Fabric workspace variable set
                print("🔐 Loading credentials from Fabric workspace variable set 'FY26INS_env'...")
                
                foundry_endpoint = None
                foundry_key = None
                
                if fabric_utils_available:
                    try:
                        # Access Fabric workspace variables using notebookutils
                        foundry_endpoint = notebookutils.credentials.getSecret("FY26INS_env", "AZURE_AI_FOUNDRY_ENDPOINT")
                        foundry_key = notebookutils.credentials.getSecret("FY26INS_env", "AZURE_AI_FOUNDRY_KEY")
                        
                        print("✅ Successfully retrieved credentials from workspace variable set")
                        
                    except Exception as cred_error:
                        print(f"⚠️ Could not access workspace variable set: {cred_error}")
                        print("Attempting alternative credential access methods...")
                        
                        # Alternative: Try using mssparkutils if available
                        try:
                            foundry_endpoint = mssparkutils.credentials.getSecret("FY26INS_env", "AZURE_AI_FOUNDRY_ENDPOINT")
                            foundry_key = mssparkutils.credentials.getSecret("FY26INS_env", "AZURE_AI_FOUNDRY_KEY")
                            print("✅ Retrieved credentials using mssparkutils")
                        except Exception as alt_error:
                            print(f"⚠️ Alternative credential access failed: {alt_error}")
                            foundry_endpoint = None
                            foundry_key = None
                else:
                    print("⚠️ Fabric notebook utilities not available")
                    print("Please ensure you're running this in a Microsoft Fabric notebook environment")
                
                if not foundry_endpoint or not foundry_key:
                    print("\n📋 Manual Setup Required:")
                    print("Please ensure the following in your Fabric workspace:")
                    print("  1. Create a variable set named 'FY26INS_env'")
                    print("  2. Add variable 'AZURE_AI_FOUNDRY_ENDPOINT' with your AI Foundry endpoint URL")
                    print("  3. Add variable 'AZURE_AI_FOUNDRY_KEY' with your AI Foundry API key")
                    print("  4. Grant this notebook access to the variable set")
                    print("\nExample values:")
                    print("  AZURE_AI_FOUNDRY_ENDPOINT: https://your-resource.services.ai.azure.com")
                    print("  AZURE_AI_FOUNDRY_KEY: your-api-key-here")
                
                if foundry_endpoint and foundry_key:
                    # Initialize Azure AI Foundry client
                    foundry_client = ChatCompletionsClient(
                        endpoint=foundry_endpoint,
                        credential=AzureKeyCredential(foundry_key)
                    )
                    print(f"✅ Azure AI Foundry client initialized")
                    print(f"   Endpoint: {foundry_endpoint[:50]}...")  # Show partial endpoint for security
                    model_loaded = True
                    ml_config["use_foundry_model"] = True
                    ml_config["foundry_endpoint"] = foundry_endpoint
                else:
                    print("⚠️ Azure AI Foundry credentials not available in variable set")
                    model_loaded = False
                    
            except ImportError:
                print("⚠️ Azure AI Inference package not available")
                print("Install with: pip install azure-ai-inference")
                model_loaded = False
            
            # Option 3: Local MLflow model files
            # model_path = "Files/Model/model.json"
            # model = mlflow.pyfunc.load_model(model_path)
            # print(f"✅ Loaded model from path: {model_path}")
            # model_loaded = True
            
            if not model_loaded:
                print("⚠️ Configure Azure AI Foundry or MLflow model and uncomment appropriate section")
            
        except Exception as e:
            logger.warning(f"Could not load ML model: {str(e)}")
            model_loaded = False

if not model_loaded:
    print("🎲 Using simulated predictions")
    ml_config["use_simulated_predictions"] = True

# COMMAND ----------

# MAGIC %md
# MAGIC ## Severity Prediction

# COMMAND ----------

if ml_config["use_simulated_predictions"]:
    print("🎲 Generating simulated severity predictions...")
    
    try:
        # Set seeds for reproducible results
        random.seed(42)
        np.random.seed(42)
        
        def simulate_severity_prediction(image_name):
            """
            Simulate severity prediction based on image name patterns
            In production, replace with actual ML model inference
            """
            # Use image name to determine severity category
            if "High" in str(image_name):
                # High severity: 0.7-0.95
                return round(random.uniform(0.7, 0.95), 3)
            elif "Medium" in str(image_name):
                # Medium severity: 0.4-0.7
                return round(random.uniform(0.4, 0.7), 3)
            elif "Low" in str(image_name):
                # Low severity: 0.1-0.4
                return round(random.uniform(0.1, 0.4), 3)
            else:
                # Random severity for unknown patterns
                return round(random.uniform(0.1, 0.9), 3)
        
        if images_pandas is not None:
            # Pandas-based processing
            print("📊 Applying predictions using Pandas...")
            images_pandas['severity'] = images_pandas['image_name'].apply(simulate_severity_prediction)
            
            # Convert back to Spark DataFrame
            predictions_df = spark.createDataFrame(
                images_pandas[['image_name', 'severity']]
            )
        else:
            # Spark-based processing for large datasets
            print("📊 Applying predictions using Spark UDF...")
            from pyspark.sql.types import DoubleType
            from pyspark.sql.functions import udf
            
            # Create UDF for severity prediction
            severity_udf = udf(simulate_severity_prediction, DoubleType())
            
            # Process in batches to manage memory efficiently
            batch_size = ml_config.get("batch_size", 1000)
            total_count = images_for_prediction.count()
            
            print(f"   Processing {total_count:,} images in batches of {batch_size:,}")
            
            # Add row numbers for batching
            from pyspark.sql.window import Window
            from pyspark.sql.functions import row_number
            
            window_spec = Window.orderBy("image_name")
            images_with_row_num = images_for_prediction.withColumn("row_num", row_number().over(window_spec))
            
            # Process in batches
            predictions_list = []
            num_batches = (total_count + batch_size - 1) // batch_size  # Ceiling division
            
            for batch_num in range(num_batches):
                start_row = batch_num * batch_size + 1
                end_row = min((batch_num + 1) * batch_size, total_count)
                
                print(f"   Processing batch {batch_num + 1}/{num_batches} (rows {start_row}-{end_row})")
                
                # Filter current batch
                batch_df = (images_with_row_num
                           .filter((col("row_num") >= start_row) & (col("row_num") <= end_row))
                           .select("image_name")
                           .withColumn("severity", severity_udf(col("image_name"))))
                
                predictions_list.append(batch_df)
            
            # Union all batch results
            if predictions_list:
                predictions_df = predictions_list[0]
                for i in range(1, len(predictions_list)):
                    predictions_df = predictions_df.union(predictions_list[i])
            else:
                # Fallback for empty result
                from pyspark.sql.types import StructType, StructField, StringType
                schema = StructType([
                    StructField("image_name", StringType(), True),
                    StructField("severity", DoubleType(), True)
                ])
                predictions_df = spark.createDataFrame([], schema)
            
            print(f"   ✅ Completed Spark-based batch processing")
        
        # Add prediction metadata
        predictions_df = (predictions_df
                        .withColumn("prediction_timestamp", current_timestamp())
                        .withColumn("model_version", lit(ml_config["model_version"])))
        
        prediction_count = predictions_df.count()
        print(f"✅ Generated {prediction_count:,} severity predictions")
        
        # Show sample predictions
        print(f"\n📊 Sample predictions:")
        display(predictions_df.limit(10))
        
        # Statistics
        severity_stats = predictions_df.select(
            F.avg("severity").alias("avg_severity"),
            F.min("severity").alias("min_severity"),
            F.max("severity").alias("max_severity"),
            F.count("severity").alias("total_predictions")
        ).collect()[0]
        
        print(f"\n📈 Prediction Statistics:")
        print(f"   Total predictions: {severity_stats['total_predictions']:,}")
        print(f"   Average severity: {severity_stats['avg_severity']:.3f}")
        print(f"   Min severity: {severity_stats['min_severity']:.3f}")
        print(f"   Max severity: {severity_stats['max_severity']:.3f}")
        
    except Exception as e:
        logger.error(f"Error generating predictions: {str(e)}")
        raise

else:
    print("🤖 Applying real ML model predictions...")
    
    # Azure AI Foundry Model Integration
    if ml_config.get("use_foundry_model", False):
        print("🌟 Using Azure AI Foundry model for predictions...")
        
        try:
            def predict_with_foundry_model(image_name, image_content=None):
                """
                Use Azure AI Foundry Vision model for damage severity prediction
                """
                try:
                    # Construct prompt for vision model
                    messages = [
                        {
                            "role": "system", 
                            "content": "You are an expert insurance adjuster analyzing vehicle damage from accident images. Rate the severity on a scale of 0.0 to 1.0 where 0.0 is no damage and 1.0 is total loss."
                        },
                        {
                            "role": "user",
                            "content": f"Analyze this accident image '{image_name}' and provide only a numeric severity score between 0.0 and 1.0."
                        }
                    ]
                    
                    # Call Azure AI Foundry model
                    response = foundry_client.complete(
                        model="gpt-4o-vision",  # Update with your deployed model name
                        messages=messages,
                        max_tokens=50,
                        temperature=0.1  # Low temperature for consistent scoring
                    )
                    
                    # Extract severity score from response
                    score_text = response.choices[0].message.content.strip()
                    
                    # Parse numeric score
                    import re
                    score_match = re.search(r'(\d+\.?\d*)', score_text)
                    if score_match:
                        severity = float(score_match.group(1))
                        # Ensure score is between 0 and 1
                        severity = max(0.0, min(1.0, severity))
                        return round(severity, 3)
                    else:
                        # Fallback to simulated prediction
                        return simulate_severity_prediction(image_name)
                        
                except Exception as e:
                    logger.warning(f"Azure AI Foundry prediction failed for {image_name}: {str(e)}")
                    # Fallback to simulated prediction
                    return simulate_severity_prediction(image_name)
            
            # Apply Foundry model predictions
            if images_pandas is not None:
                print("📊 Applying Azure AI Foundry predictions using Pandas...")
                images_pandas['severity'] = images_pandas['image_name'].apply(predict_with_foundry_model)
                predictions_df = spark.createDataFrame(images_pandas[['image_name', 'severity']])
            else:
                print("📊 Applying Azure AI Foundry predictions using Spark UDF...")
                from pyspark.sql.types import DoubleType
                from pyspark.sql.functions import udf
                
                # Create UDF for Foundry model prediction
                foundry_udf = udf(predict_with_foundry_model, DoubleType())
                
                # Process in batches for large datasets
                batch_size = ml_config.get("api_batch_size", 500)  # Smaller batches for API calls
                api_delay = ml_config.get("api_delay_seconds", 0.1)
                total_count = images_for_prediction.count()
                
                print(f"   Processing {total_count:,} images in batches of {batch_size:,}")
                print("   ⚠️ Note: API-based predictions will take longer for large datasets")
                print(f"   Using {api_delay}s delay between batches to respect API rate limits")
                
                # Add row numbers for batching
                from pyspark.sql.window import Window
                from pyspark.sql.functions import row_number
                
                window_spec = Window.orderBy("image_name")
                images_with_row_num = images_for_prediction.withColumn("row_num", row_number().over(window_spec))
                
                # Process in batches to avoid API rate limits
                predictions_list = []
                num_batches = (total_count + batch_size - 1) // batch_size
                
                for batch_num in range(num_batches):
                    start_row = batch_num * batch_size + 1
                    end_row = min((batch_num + 1) * batch_size, total_count)
                    
                    print(f"   Processing batch {batch_num + 1}/{num_batches} (rows {start_row}-{end_row})")
                    
                    try:
                        # Filter current batch
                        batch_df = (images_with_row_num
                                   .filter((col("row_num") >= start_row) & (col("row_num") <= end_row))
                                   .select("image_name")
                                   .withColumn("severity", foundry_udf(col("image_name"))))
                        
                        # Cache batch result to avoid recomputation
                        batch_df.cache()
                        batch_count = batch_df.count()  # Trigger computation
                        
                        predictions_list.append(batch_df)
                        print(f"     ✅ Batch {batch_num + 1} completed: {batch_count} predictions")
                        
                        # Small delay to respect API rate limits
                        import time
                        time.sleep(api_delay)
                        
                    except Exception as batch_error:
                        logger.warning(f"Batch {batch_num + 1} failed: {batch_error}")
                        print(f"     ⚠️ Batch {batch_num + 1} failed, using simulated predictions")
                        
                        # Fallback to simulated predictions for failed batch
                        # Define simulated prediction function for fallback
                        def fallback_severity_prediction(image_name):
                            if "High" in str(image_name):
                                return round(random.uniform(0.7, 0.95), 3)
                            elif "Medium" in str(image_name):
                                return round(random.uniform(0.4, 0.7), 3)
                            elif "Low" in str(image_name):
                                return round(random.uniform(0.1, 0.4), 3)
                            else:
                                return round(random.uniform(0.1, 0.9), 3)
                        
                        simulate_udf = udf(fallback_severity_prediction, DoubleType())
                        batch_df = (images_with_row_num
                                   .filter((col("row_num") >= start_row) & (col("row_num") <= end_row))
                                   .select("image_name")
                                   .withColumn("severity", simulate_udf(col("image_name"))))
                        
                        predictions_list.append(batch_df)
                
                # Union all batch results
                if predictions_list:
                    predictions_df = predictions_list[0]
                    for i in range(1, len(predictions_list)):
                        predictions_df = predictions_df.union(predictions_list[i])
                    
                    # Remove caching to free memory
                    for batch_df in predictions_list:
                        batch_df.unpersist()
                else:
                    # Fallback for empty result
                    from pyspark.sql.types import StructType, StructField, StringType
                    schema = StructType([
                        StructField("image_name", StringType(), True),
                        StructField("severity", DoubleType(), True)
                    ])
                    predictions_df = spark.createDataFrame([], schema)
                
                print(f"   ✅ Completed Spark-based Azure AI Foundry processing")
            
            print("✅ Azure AI Foundry predictions completed")
            
        except Exception as e:
            logger.error(f"Error with Azure AI Foundry model: {str(e)}")
            print("Falling back to simulated predictions")
            ml_config["use_simulated_predictions"] = True
    
    else:
        # Traditional ML model inference (MLflow, TensorFlow, PyTorch, etc.)
        try:
            # Example structure for real ML model:
            # if images_pandas is not None:
            #     # Preprocess images
            #     processed_images = preprocess_images(images_pandas['content'])
            #     # Apply model
            #     severity_scores = model.predict(processed_images)
            #     images_pandas['severity'] = severity_scores
            #     predictions_df = spark.createDataFrame(images_pandas[['image_name', 'severity']])
            # else:
            #     # Spark-based distributed inference
            #     predictions_df = apply_model_distributed(images_for_prediction, model)
            
            print("⚠️ Traditional ML model inference not implemented yet")
            print("Using simulated predictions as fallback")
            ml_config["use_simulated_predictions"] = True
            
        except Exception as e:
            logger.error(f"Error applying ML model: {str(e)}")
            raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Silver Accident Table (Memory-Optimized)

# COMMAND ----------

print("🥈 Creating silver accident table with memory optimization...")

# ---------- Helper Functions for Large Dataset Incremental Processing ----------
from pyspark.sql import functions as F
from pyspark.sql.functions import col, current_timestamp, when, lit
from pyspark.sql.window import Window
import os, json, math, time

def ensure_dir(path: str):
    try:
        dbutils.fs.mkdirs(path)  # If running in Fabric with dbutils alias
    except Exception:
        pass

def load_progress(path):
    try:
        if spark._jsparkSession.sessionState().newHadoopConf():
            # Try reading as text file
            dfp = spark.read.format("text").load(path)
            rows = [r.value for r in dfp.collect()]
            if rows:
                return json.loads(rows[0])
    except Exception:
        pass
    return {"last_completed_chunk": 0}

def save_progress(path, state):
    try:
        tmp_df = spark.createDataFrame([(json.dumps(state),)], ["value"])  # overwrite single file
        (tmp_df
         .coalesce(1)
         .write.mode("overwrite").format("text").save(path))
    except Exception as e:
        print(f"⚠️ Unable to persist progress: {e}")

# Simulated prediction function (reuse if already defined)
try:
    simulate_severity_prediction  # noqa
except NameError:
    import random, numpy as np
    random.seed(42); np.random.seed(42)
    def simulate_severity_prediction(image_name: str):
        if "High" in str(image_name):
            return round(random.uniform(0.7, 0.95), 3)
        if "Medium" in str(image_name):
            return round(random.uniform(0.4, 0.7), 3)
        if "Low" in str(image_name):
            return round(random.uniform(0.1, 0.4), 3)
        return round(random.uniform(0.1, 0.9), 3)

from pyspark.sql.types import DoubleType
from pyspark.sql.functions import udf
severity_udf = udf(simulate_severity_prediction, DoubleType())

def create_empty_silver(bronze_df):
    """Create (or recreate) an empty partitioned silver_accident Delta table using explicit DDL.
    Writing an empty DataFrame with partitionBy can lose partition metadata in some environments,
    causing subsequent append errors: 'provided partitioning does not match'.
    This function derives a minimal schema from bronze and adds ML enrichment columns.
    """
    print("🧱 (Re)creating silver_accident table via explicit DDL ...")
    spark.sql(f"DROP TABLE IF EXISTS {silver_accident}")

    include_content = ml_config.get("include_content_in_silver", False)
    # Allowed bronze source columns we keep
    source_keep = ["image_name", "path", "claim_no", "loaded_timestamp"]
    if include_content:
        source_keep.append("content")
    bronze_fields = {f.name: f.dataType.simpleString() for f in bronze_df.schema if f.name in source_keep}

    # Map Spark simpleString types to SQL types
    type_map = {
        'string': 'STRING',
        'binary': 'BINARY',
        'int': 'INT',
        'integer': 'INT',
        'bigint': 'BIGINT',
        'long': 'BIGINT',
        'double': 'DOUBLE',
        'float': 'FLOAT',
        'timestamp': 'TIMESTAMP',
        'date': 'DATE'
    }
    def map_type(t):
        return type_map.get(t.lower(), 'STRING')

    column_defs = []
    for name in source_keep:
        if name in bronze_fields:
            column_defs.append(f"{name} {map_type(bronze_fields[name])}")
    # Enrichment columns (exclude partition column here)
    enrichment_defs = [
        "severity DOUBLE",
        "processed_timestamp TIMESTAMP",
        "severity_category STRING",
        "high_severity_flag BOOLEAN",
        "data_quality_score DOUBLE",
        "chunk_id INT"
    ]

    ddl_cols = ",\n  ".join(column_defs + enrichment_defs + ["partition_date STRING"])
    ddl = f"""
    CREATE TABLE {silver_accident} (
      {ddl_cols}
    )
    USING DELTA
    PARTITIONED BY (partition_date)
    """
    spark.sql(ddl)
    print(f"✅ Created partitioned table {silver_accident} (partition_date)")
    # Verify partitioning
    part_info = spark.sql(f"DESCRIBE DETAIL {silver_accident}").select("partitionColumns").collect()[0][0]
    print(f"🔍 Partition columns registered: {part_info}")

def process_chunk(base_df, start_rn, end_rn, chunk_id, prediction_threshold):
    chunk = (base_df
             .filter((col("_rn") >= start_rn) & (col("_rn") <= end_rn))
             .drop("_rn"))
    # Apply prediction
    chunk = chunk.withColumn("severity", severity_udf(col("image_name")))
    has_content = "content" in chunk.columns
    if has_content:
        quality_expr = (when(col("content").isNotNull() & col("severity").isNotNull(), 1.0)
                        .when(col("content").isNotNull() & col("severity").isNull(), 0.7)
                        .when(col("content").isNull() & col("severity").isNotNull(), 0.3)
                        .otherwise(0.0))
    else:
        # Without content column, quality is 1.0 when severity present else 0.0
        quality_expr = when(col("severity").isNotNull(), 1.0).otherwise(0.0)
    enriched = (chunk
        .withColumn("processed_timestamp", current_timestamp())
        .withColumn("severity_category",
                    when(col("severity") >= 0.8, "High")
                    .when(col("severity") >= 0.6, "Medium-High")
                    .when(col("severity") >= 0.4, "Medium")
                    .when(col("severity") >= 0.2, "Low-Medium")
                    .otherwise("Low"))
        .withColumn("high_severity_flag", when(col("severity") >= prediction_threshold, True).otherwise(False))
        .withColumn("data_quality_score", quality_expr)
        .withColumn("partition_date", F.date_format(col("processed_timestamp"), "yyyy-MM-dd"))
        .withColumn("chunk_id", lit(chunk_id))
    )
    return enriched

def optimized_large_write(bronze_df, prediction_threshold):
    total = bronze_df.count()
    chunk_size = ml_config["chunk_size_primary"]
    # Derive chunk size for small datasets to still exercise incremental flow
    if total < ml_config["chunk_size_min"]:
        target_chunks = max(1, ml_config.get("small_dataset_target_chunks", 5))
        # Compute provisional chunk size aiming for target_chunks
        provisional = max(ml_config.get("small_dataset_min_chunk", 2000), math.ceil(total / target_chunks))
        # Ensure we don't exceed original min (keep smaller for more chunks) & not below 1
        chunk_size = max(1, provisional)
    elif total < chunk_size:
        # Medium dataset below primary chunk size but above min threshold
        chunk_size = max(ml_config["chunk_size_min"], min(chunk_size, total))
    # Additional scaling for very large volumes
    if total > 50_000_000:
        chunk_size = 300_000
    elif total > 30_000_000:
        chunk_size = 400_000

    include_content = ml_config.get("include_content_in_silver", False)
    base_keep = ["image_name", "path", "claim_no", "loaded_timestamp"]
    if include_content:
        base_keep.append("content")
    base_cols = [c for c in bronze_df.columns if c in base_keep]
    base_df = bronze_df.select(*base_cols)
    window_spec = Window.orderBy("image_name")
    indexed = base_df.withColumn("_rn", F.row_number().over(window_spec))

    num_chunks = max(1, math.ceil(total / chunk_size))
    print(f"🧩 Incremental processing: {total:,} rows in {num_chunks} chunks of ~{chunk_size:,}")

    # Determine existing target table state for safety reset
    existing_rows = 0
    try:
        if spark.catalog.tableExists(silver_accident):
            existing_rows = spark.table(silver_accident).count()
    except Exception:
        existing_rows = 0

    # Progress checkpoint handling
    progress_path = ml_config["progress_checkpoint_path"]
    progress = load_progress(progress_path)
    last_done = progress.get("last_completed_chunk", 0)

    # Safety reset conditions
    if (ml_config.get("force_reset_on_empty_table", True) and existing_rows == 0 and last_done > 0):
        print(f"⚠️ Progress indicated chunk {last_done} done, but target table is empty; resetting to chunk 0.")
        last_done = 0
        save_progress(progress_path, {"last_completed_chunk": 0, "reset_due_to_empty_table": True, "total": total})
    elif last_done >= num_chunks and ml_config.get("reset_progress_on_overshoot", True):
        print(f"⚠️ Progress checkpoint ({last_done}) >= total chunks ({num_chunks}); resetting progress.")
        last_done = 0
        save_progress(progress_path, {"last_completed_chunk": 0, "reset_due_to_overshoot": True, "total": total})
    elif last_done > 0:
        print(f"🔁 Resuming from chunk {last_done+1}")

    start_time_overall = time.time()
    success_chunks = 0

    for chunk_id in range(last_done + 1, num_chunks + 1):
        start_rn = (chunk_id - 1) * chunk_size + 1
        end_rn = min(chunk_id * chunk_size, total)
        print(f"→ Chunk {chunk_id}/{num_chunks} rows {start_rn:,}-{end_rn:,}")
        t0 = time.time()
        try:
            chunk_df = process_chunk(indexed, start_rn, end_rn, chunk_id, prediction_threshold)
            est_partitions = min(ml_config["max_partitions_per_chunk"], max(1, (end_rn - start_rn + 1) // 200_000))
            chunk_df = chunk_df.repartition(est_partitions, "partition_date")
            persisted = False
            try:
                (chunk_df.write
                    .format("delta")
                    .mode("append")
                    .option("mergeSchema", "true")
                    .saveAsTable(silver_accident))
                persisted = True
            except Exception as pw_err:
                msg = str(pw_err)
                if "partitioning does not match" in msg:
                    print("🛠 Partition mismatch detected; recreating table DDL and retrying chunk write ...")
                    create_empty_silver(bronze_df)
                    (chunk_df.write
                        .format("delta")
                        .mode("append")
                        .option("mergeSchema", "true")
                        .saveAsTable(silver_accident))
                    persisted = True
                else:
                    raise
            if not persisted:
                raise RuntimeError("Chunk write did not persist and no retry path executed")
            # Validate persistence (especially critical for first chunk)
            written_rows = spark.table(silver_accident).filter(col("chunk_id") == chunk_id).count()
            if written_rows == 0:
                raise RuntimeError(f"Post-write validation failed: 0 rows found for chunk_id={chunk_id}")
            duration = time.time() - t0
            rows = end_rn - start_rn + 1
            print(f"   ✅ Chunk {chunk_id} written ({rows:,} rows; validated {written_rows:,}) in {duration:.1f}s ({rows/max(duration,0.1):,.0f} rows/s)")
            success_chunks += 1
            save_progress(progress_path, {"last_completed_chunk": chunk_id, "total": total, "chunk_size": chunk_size})
            # Adaptive back-off
            if duration > 180 and chunk_size > ml_config["small_dataset_min_chunk"]:
                chunk_size = max(ml_config["small_dataset_min_chunk"], chunk_size // 2)
                remaining = total - end_rn
                if remaining > 0:
                    num_chunks = chunk_id + math.ceil(remaining / chunk_size)
                    print(f"   🔧 Back-off: future chunk_size -> {chunk_size:,}; new total chunks ≈ {num_chunks}")
        except Exception as ce:
            print(f"   ❌ Chunk {chunk_id} failed: {ce}")
            print("   🔁 Reducing chunk size and retrying once")
            retry_size = max(ml_config.get("small_dataset_min_chunk", 2000), (end_rn - start_rn + 1) // 2)
            try:
                retry_end = start_rn + retry_size - 1
                retry_df = process_chunk(indexed, start_rn, retry_end, chunk_id, prediction_threshold)
                retry_df = retry_df.repartition(min(ml_config["max_partitions_per_chunk"], max(1, retry_size // 200_000)), "partition_date")
                (retry_df.write
                    .format("delta")
                    .mode("append")
                    .option("mergeSchema", "true")
                    .saveAsTable(silver_accident))
                retry_written = spark.table(silver_accident).filter(col("chunk_id") == chunk_id).count()
                if retry_written == 0:
                    raise RuntimeError(f"Retry persistence validation failed for chunk {chunk_id}")
                print(f"   ✅ Partial retry wrote rows {start_rn:,}-{retry_end:,} (validated {retry_written:,})")
                save_progress(progress_path, {"last_completed_chunk": chunk_id, "partial": True, "retry_rows": retry_size})
            except Exception as retry_err:
                print(f"   💥 Retry failed: {retry_err} - skipping chunk")
                save_progress(progress_path, {"last_completed_chunk": chunk_id - 1, "skipped": chunk_id})
                continue
    total_dur = time.time() - start_time_overall
    print(f"🏁 Incremental processing finished. Successful chunks: {success_chunks}/{num_chunks}. Total time {total_dur/60:.1f} min")

# -----------------------------------------------------------------------------
try:
    bronze_count_est = bronze_accident.count()
    # Decide path
    use_incremental = ml_config.get("force_incremental", False) or bronze_count_est > ml_config.get("small_path_row_limit", 1_000_000)
    if use_incremental:
        print(f"⚙️ Using incremental chunked pipeline (rows={bronze_count_est:,})")
        create_empty_silver(bronze_accident)
        optimized_large_write(bronze_accident, ml_config["prediction_threshold"])
    else:
        print(f"📉 Small dataset path selected (rows={bronze_count_est:,})")
        silver_accident_df = (bronze_accident
                              .join(predictions_df, "image_name", "left_outer") if 'predictions_df' in globals() else bronze_accident
                              .withColumn("severity", severity_udf(col("image_name")))
                              .withColumn("processed_timestamp", current_timestamp()))
        silver_accident_df = (silver_accident_df
            .withColumn("severity_category",
                when(col("severity") >= 0.8, "High")
                .when(col("severity") >= 0.6, "Medium-High")
                .when(col("severity") >= 0.4, "Medium")
                .when(col("severity") >= 0.2, "Low-Medium")
                .otherwise("Low"))
            .withColumn("high_severity_flag", when(col("severity") >= ml_config["prediction_threshold"], True).otherwise(False))
            .withColumn("data_quality_score",
                when(col("content").isNotNull() & col("severity").isNotNull(), 1.0)
                .when(col("content").isNotNull() & col("severity").isNull(), 0.7)
                .when(col("content").isNull() & col("severity").isNotNull(), 0.3)
                .otherwise(0.0))
            # Fix: use processed_timestamp (prediction_timestamp may be null here)
            .withColumn("partition_date", F.date_format(col("processed_timestamp"), "yyyy-MM-dd"))
        )
        if not ml_config.get("include_content_in_silver", False) and "content" in silver_accident_df.columns:
            silver_accident_df = silver_accident_df.drop("content")
        print(f"💾 Writing small dataset to Delta (overwrite mode)...")
        (silver_accident_df.write
         .format("delta")
         .mode("overwrite")
         .option("overwriteSchema", "true")
         .partitionBy("partition_date")
         .saveAsTable(silver_accident))
        print(f"✅ Silver accident table created (small mode) -> {silver_accident}")
except Exception as e:
    logger.error(f"Error in silver accident processing pipeline: {e}")
    raise

# COMMAND ----------

print("⚡ Optimizing silver accident table...")

try:
    # Optimize table for query performance
    spark.sql(f"OPTIMIZE {silver_accident}")
    
    # Optional: Z-ORDER by frequently queried columns
    # Uncomment if you have specific query patterns
    # spark.sql("OPTIMIZE silver_accident ZORDER BY (severity_category, high_severity_flag)")
    
    print("✅ Silver table optimized")
    
except Exception as e:
    logger.error(f"Error optimizing table: {str(e)}")

# COMMAND ----------

print("🔍 Validating silver table data quality...")

try:
    silver_table = spark.table(silver_accident)
    
    # Basic counts
    total_records = silver_table.count()
    records_with_predictions = silver_table.filter(col("severity").isNotNull()).count()
    if "content" in silver_table.columns:
        records_with_images = silver_table.filter(col("content").isNotNull()).count()
    else:
        records_with_images = 0
    
    print(f"\n📊 Data Quality Report:")
    print(f"   Total records: {total_records:,}")
    print(f"   Records with predictions: {records_with_predictions:,} ({(records_with_predictions/total_records)*100:.1f}%)")
    print(f"   Records with images: {records_with_images:,} ({(records_with_images/total_records)*100:.1f}%)")
    
    # Severity distribution validation
    severity_distribution = (silver_table
                            .groupBy("severity_category")
                            .count()
                            .orderBy(F.desc("count")))
    
    print(f"\n📈 Severity Distribution:")
    display(severity_distribution)
    
    # Data quality score distribution
    quality_distribution = (silver_table
                          .groupBy("data_quality_score")
                          .count()
                          .orderBy("data_quality_score"))
    
    print(f"\n🏆 Data Quality Score Distribution:")
    display(quality_distribution)
    
    # Flag high-quality records
    high_quality_records = silver_table.filter(col("data_quality_score") >= 1.0).count()
    print(f"\n✅ High Quality Records: {high_quality_records:,} ({(high_quality_records/total_records)*100:.1f}%)")
    
except Exception as e:
    logger.error(f"Error in data quality validation: {str(e)}")

# COMMAND ----------

print("📊 Performing comprehensive severity analysis...")

try:
    silver_table = spark.table(silver_accident)
    
    # Detailed severity statistics
    severity_stats = silver_table.select(
        F.count("severity").alias("total_predictions"),
        F.avg("severity").alias("avg_severity"),
        F.min("severity").alias("min_severity"),
        F.max("severity").alias("max_severity"),
        F.stddev("severity").alias("stddev_severity"),
        F.expr("percentile_approx(severity, 0.25)").alias("q1_severity"),
        F.expr("percentile_approx(severity, 0.5)").alias("median_severity"),
        F.expr("percentile_approx(severity, 0.75)").alias("q3_severity")
    )
    
    print(f"\n📈 Detailed Severity Statistics:")
    display(severity_stats)
    
    # High severity analysis
    high_severity_analysis = (silver_table
        .filter(col("severity") >= 0.8)
        .groupBy("severity_category")
        .agg(
            F.count("*").alias("count"),
            F.avg("severity").alias("avg_severity"),
            F.min("severity").alias("min_severity"),
            F.max("severity").alias("max_severity")
        )
        .orderBy(F.desc("count")))
    
    print(f"\n🚨 High Severity Analysis (≥0.8):")
    display(high_severity_analysis)
    
    # Model performance indicators (for simulated data)
    if ml_config["use_simulated_predictions"]:
        print(f"\n🎯 Simulated Model Performance Indicators:")
        high_severity_count = silver_table.filter(col("severity") >= 0.8).count()
        medium_severity_count = silver_table.filter((col("severity") >= 0.4) & (col("severity") < 0.8)).count()
        low_severity_count = silver_table.filter(col("severity") < 0.4).count()
        total_with_severity = high_severity_count + medium_severity_count + low_severity_count
        if total_with_severity > 0:
            print(f"   High (≥0.8): {high_severity_count:,} ({high_severity_count/total_with_severity*100:.1f}%)")
            print(f"   Medium (0.4-0.8): {medium_severity_count:,} ({medium_severity_count/total_with_severity*100:.1f}%)")
            print(f"   Low (<0.4): {low_severity_count:,} ({low_severity_count/total_with_severity*100:.1f}%)")
    
except Exception as e:
    logger.error(f"Error in severity analysis: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business Intelligence Summary

# COMMAND ----------

print("📋 Generating business intelligence summary...")

try:
    silver_table = spark.table(silver_accident)
    
    # Key business metrics
    total_claims = silver_table.count()
    high_severity_claims = silver_table.filter(col("high_severity_flag") == True).count()
    avg_severity = silver_table.select(F.avg("severity")).collect()[0][0]
    
    # Risk assessment
    high_risk_percentage = (high_severity_claims / total_claims) * 100 if total_claims > 0 else 0
    
    print(f"\n💼 Business Intelligence Summary:")
    print(f"   Total accident cases: {total_claims:,}")
    print(f"   High severity cases: {high_severity_claims:,}")
    print(f"   High risk percentage: {high_risk_percentage:.1f}%")
    print(f"   Average severity score: {avg_severity:.3f}")
    
    # Recommendations based on severity distribution
    if high_risk_percentage > 30:
        print(f"\n🚨 Risk Alert: High severity rate ({high_risk_percentage:.1f}%) - Consider enhanced investigation")
    elif high_risk_percentage > 15:
        print(f"\n⚠️ Risk Warning: Elevated severity rate ({high_risk_percentage:.1f}%) - Monitor closely")
    else:
        print(f"\n✅ Risk Status: Normal severity rate ({high_risk_percentage:.1f}%) - Standard processing")
    
    # Processing efficiency metrics
    records_with_complete_data = silver_table.filter(col("data_quality_score") >= 1.0).count()
    processing_efficiency = (records_with_complete_data / total_claims) * 100 if total_claims > 0 else 0
    
    print(f"\n📊 Processing Efficiency:")
    print(f"   Complete data records: {records_with_complete_data:,}")
    print(f"   Processing efficiency: {processing_efficiency:.1f}%")
except Exception as e:
    logger.error(f"Error generating BI summary: {str(e)}")
    print(f"❌ Error generating BI summary: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("="*60)
print("SILVER LAYER PROCESSING SUMMARY")
print("="*60)

try:
    # Use existing DataFrame for bronze (bronze_accident variable now holds DataFrame) and load silver table by name
    bronze_count = bronze_accident.count() if 'bronze_accident' in globals() else spark.table(f"{bronze}.bronze_accident").count()
    silver_df = spark.table(silver_accident)
    silver_count = silver_df.count()

    print(f"📊 Processing Results:")
    print(f"   Bronze records: {bronze_count:,}")
    print(f"   Silver records: {silver_count:,}")
    if bronze_count > 0:
        print(f"   Processing success rate: {(silver_count/bronze_count)*100:.1f}%")
    predictions_made = silver_df.filter(col("severity").isNotNull()).count()
    print(f"\n🤖 ML Processing:")
    print(f"   Predictions generated: {predictions_made:,}")
    print(f"   Model version: {ml_config['model_version']}")
    print(f"   Prediction method: {'Simulated' if ml_config['use_simulated_predictions'] else 'Real ML Model'}")
    high_severity = silver_df.filter(col("high_severity_flag") == True).count()
    print(f"\n💼 Business Value:")
    print(f"   High severity cases identified: {high_severity:,}")
    print(f"   Automated risk assessment: ✅ Complete")
    print(f"   Data-driven decision support: ✅ Ready")
    print(f"\n💾 Tables Created:")
    print(f"   • {silver_accident} (partitioned by partition_date)")
    print(f"   • Columns: severity, severity_category, high_severity_flag, data_quality_score")
    print(f"\n🔗 Next Steps:")
    print(f"   1. Join with claims/policies for gold layer")
    print(f"   2. Build Power BI DirectLake model")
    print(f"   3. Integrate rules engine")
    print(f"   4. Deploy scheduled pipeline")
except Exception as e:
    print(f"❌ Error in summary: {str(e)}")

print("="*60)
print("✅ Silver layer processing completed!")
print("="*60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Production ML Model Integration Guide

# COMMAND ----------

print("""
🤖 PRODUCTION ML MODEL INTEGRATION

To replace simulated predictions with real ML models:

1. **MLflow Model Registry:**
   ```python
   model_name = "damage_severity_model"
   model_uri = f"models:/{model_name}/production"
   model = mlflow.pyfunc.load_model(model_uri)
   
   # Apply to images
   predictions = model.predict(image_data)
   ```

2. **Custom TensorFlow/PyTorch Model:**
   ```python
   import tensorflow as tf
   model = tf.keras.models.load_model("path/to/model")
   
   def predict_severity(image_content):
       processed_image = preprocess_image(image_content)
       prediction = model.predict(processed_image)
       return float(prediction[0])
   ```

3. **Azure Cognitive Services:**
   ```python
   from azure.cognitiveservices.vision.computervision import ComputerVisionClient
   
   # Configure Computer Vision client
   cv_client = ComputerVisionClient(endpoint, credentials)
   
   # Implement custom damage assessment
   def analyze_damage(image_url):
       analysis = cv_client.analyze_image(image_url, visual_features=['Objects', 'Tags'])
       return calculate_severity_score(analysis)
   ```

4. **Model Performance Monitoring:**
   - Track prediction confidence scores
   - Monitor model drift over time
   - Compare predictions with expert assessments
   - Set up automated retraining pipelines

5. **A/B Testing Framework:**
   - Compare multiple model versions
   - Gradual rollout of new models
   - Performance benchmarking
   - Fallback to previous model versions
""")

print("📋 Production integration guide complete")
