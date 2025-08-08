# Configure paths for accident images and metadata
# Update these paths based on your lakehouse structure
accidents_path = "Files/data_sources/Accidents"
metadata_path = "Files/data_sources/Accidents/image_metadata.csv"

# Bronze schema configuration
bronze = "bronze"
try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {bronze}")
    print(f"✅ Ensured schema '{bronze}' exists")
except Exception as e:
    print(f"⚠️ Schema creation skipped/failed: {e}")

print(f"📁 Accidents path: {accidents_path}")
print(f"📄 Metadata path: {metadata_path}")

# COMMAND ----------

import pandas as pd
import logging
from pyspark.sql import functions as F
from pyspark.sql.functions import split, size, col, input_file_name, current_timestamp

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

print("✅ Libraries imported successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Image Metadata

# COMMAND ----------

print("📄 Loading image metadata...")

try:
    # Read the metadata CSV file
    metadata_df = (spark.read
                   .format("csv")
                   .option("header", "true")
                   .option("inferSchema", "true")
                   .load(metadata_path))
    
    metadata_count = metadata_df.count()
    print(f"✅ Loaded {metadata_count:,} metadata records")
    
    # Show metadata schema and sample
    print(f"\n📋 Metadata schema:")
    metadata_df.printSchema()
    
    print(f"\n📄 Sample metadata:")
    display(metadata_df.limit(5))
    
except Exception as e:
    logger.error(f"Error loading metadata: {str(e)}")
    print(f"Please ensure metadata file exists at: {metadata_path}")
    raise

# COMMAND ----------

print(" Loading accident images...")

try:
    # Read images as binary files
    images_df = (spark.read
                 .format("binaryFile")
                 .load(accidents_path))
    
    image_count = images_df.count()
    print(f"✅ Loaded {image_count:,} images")
    
    # Extract image name from file path for joining with metadata
    split_col = split(images_df['path'], '/')
    images_with_names_df = images_df.withColumn(
        'image_name', 
        split_col.getItem(size(split_col) - 1)
    )
    
    # Show image data structure
    print(f"\n📋 Image data schema:")
    images_with_names_df.printSchema()
    
    print(f"\n📄 Sample image data:")
    display(images_with_names_df.select("path", "image_name").limit(5))
    
except Exception as e:
    logger.error(f"Error loading images: {str(e)}")
    print(f"Please ensure image files exist at: {accidents_path}")
    raise


# COMMAND ----------

print("🔍 Performing data quality validation...")

try:
    # Check metadata quality
    print(f"\n📊 Metadata Quality Checks:")
    total_metadata = metadata_df.count()
    
    # Check for null values in key columns
    if 'image_name' in metadata_df.columns:
        null_image_names = metadata_df.filter(F.col("image_name").isNull()).count()
        print(f"   Null image names: {null_image_names:,} ({(null_image_names/total_metadata)*100:.1f}%)")
    
    # Check for duplicate image names
    distinct_image_names = metadata_df.select("image_name").distinct().count()
    duplicate_count = total_metadata - distinct_image_names
    print(f"   Duplicate image names: {duplicate_count:,}")
    
    # Check image quality
    print(f"\n📸 Image Quality Checks:")
    total_images = images_with_names_df.count()
    
    # Check for images with no content
    empty_images = images_with_names_df.filter(F.col("content").isNull()).count()
    print(f"   Empty images: {empty_images:,} ({(empty_images/total_images)*100:.1f}%)")
    
    # Check join coverage
    joined_count = (metadata_df.join(
        images_with_names_df.select("image_name"),
        "image_name",
        "inner"
    ).count())
    
    print(f"\n🔗 Join Quality:")
    print(f"   Metadata records: {total_metadata:,}")
    print(f"   Image files: {total_images:,}")
    print(f"   Successful joins: {joined_count:,}")
    print(f"   Join success rate: {(joined_count/total_metadata)*100:.1f}%")
    
    # Warn about missing joins
    if joined_count < total_metadata:
        missing_images = total_metadata - joined_count
        print(f"   ⚠️ Missing image files: {missing_images:,}")
    
    if joined_count < total_images:
        orphaned_images = total_images - joined_count
        print(f"   ⚠️ Orphaned image files (no metadata): {orphaned_images:,}")
    
except Exception as e:
    logger.error(f"Error in data quality validation: {str(e)}")

# COMMAND ----------

print("Creating bronze image metadata table...")

try:
    # Add processing metadata to metadata table
    bronze_metadata_df = metadata_df.withColumn(
        "loaded_timestamp", 
        current_timestamp()
    ).withColumn(
        "source_file",
        F.lit(metadata_path)
    )
    
    # Save as Delta table
    (bronze_metadata_df.write
     .format("delta")
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .saveAsTable(f"{bronze}.bronze_image_metadata"))
    
    print("✅ Bronze image metadata table created successfully")
    
    # Show sample
    print(f"\n📄 Sample bronze metadata:")
    display(bronze_metadata_df.limit(5))
    
except Exception as e:
    logger.error(f"Error creating bronze metadata table: {str(e)}")
    raise

# COMMAND ----------

print("Creating bronze images table...")

try:
    # Add processing metadata to images
    bronze_images_df = (images_with_names_df
                       .withColumn("loaded_timestamp", current_timestamp())
                       .withColumn("file_size", F.length(F.col("content")))
                       .withColumn("source_path", F.lit(accidents_path)))
    
    # Save as Delta table
    (bronze_images_df.write
     .format("delta")
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .saveAsTable(f"{bronze}.bronze_images"))
    
    print("✅ Bronze images table created successfully")
    
    # Show sample (without binary content)
    print(f"\n📄 Sample bronze images:")
    display(bronze_images_df.select(
        "image_name", "path", "file_size", "loaded_timestamp"
    ).limit(5))
    
except Exception as e:
    logger.error(f"Error creating bronze images table: {str(e)}")
    raise


# COMMAND ----------

print("🥉 Creating combined bronze accident table...")

try:
    # Join metadata with image data for complete bronze table
    bronze_accident_df = (metadata_df.join(
        images_with_names_df,
        metadata_df.image_name == images_with_names_df.image_name,
        "left_outer"
    ).drop(images_with_names_df.image_name))
    
    # Add processing metadata
    bronze_accident_df = bronze_accident_df.withColumn(
        "loaded_timestamp", 
        current_timestamp()
    ).withColumn(
        "source_metadata_file",
        F.lit(metadata_path)
    ).withColumn(
        "source_images_path", 
        F.lit(accidents_path)
    )
    
    # Count joined records
    bronze_count = bronze_accident_df.count()
    print(f"📊 Bronze accident table contains {bronze_count:,} records")
    
    # Save as Delta table
    (bronze_accident_df.write
     .format("delta")
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .saveAsTable(f"{bronze}.bronze_accident"))
    
    print("✅ Bronze accident table created successfully")
    
    # Show sample (without binary content)
    print(f"\n📄 Sample bronze accident data:")
    display(bronze_accident_df.select(
        "image_name", "path", "loaded_timestamp"
    ).limit(5))
    
except Exception as e:
    logger.error(f"Error creating bronze accident table: {str(e)}")
    raise


# COMMAND ----------

print("⚡ Optimizing bronze tables...")

try:
    # Optimize all bronze tables for better query performance
    tables_to_optimize = [
        f"{bronze}.bronze_image_metadata",
        f"{bronze}.bronze_images",
        f"{bronze}.bronze_accident"
    ]
    
    for table_name in tables_to_optimize:
        print(f"   Optimizing {table_name}...")
        spark.sql(f"OPTIMIZE {table_name}")
        print(f"   ✅ {table_name} optimized")
    
    print("✅ All bronze tables optimized")
    
except Exception as e:
    logger.error(f"Error optimizing tables: {str(e)}")


# COMMAND ----------

print("📊 Validating bronze tables...")

try:
    # Get table statistics
    bronze_metadata = spark.table(f"{bronze}.bronze_image_metadata")
    bronze_images = spark.table(f"{bronze}.bronze_images")
    bronze_accident = spark.table(f"{bronze}.bronze_accident")
    
    metadata_count = bronze_metadata.count()
    images_count = bronze_images.count()
    accident_count = bronze_accident.count()
    
    print(f"\n📊 Bronze Table Statistics:")
    print(f"   bronze_image_metadata: {metadata_count:,} records")
    print(f"   bronze_images: {images_count:,} records")
    print(f"   bronze_accident: {accident_count:,} records")
    
    # Data quality summary
    images_with_content = bronze_images.filter(F.col("content").isNotNull()).count()
    content_availability = (images_with_content / images_count) * 100 if images_count > 0 else 0
    
    print(f"\n✅ Data Quality Summary:")
    print(f"   Images with content: {images_with_content:,} ({content_availability:.1f}%)")
    print(f"   Metadata coverage: {(metadata_count/accident_count)*100:.1f}%" if accident_count > 0 else "   Metadata coverage: N/A")
    
    # Schema validation
    print(f"\n📋 Schema Validation:")
    print(f"   bronze_image_metadata columns: {len(bronze_metadata.columns)}")
    print(f"   bronze_images columns: {len(bronze_images.columns)}")
    print(f"   bronze_accident columns: {len(bronze_accident.columns)}")
    
    # Show final schemas
    print(f"\n📋 Bronze Accident Schema:")
    bronze_accident.printSchema()
    
except Exception as e:
    logger.error(f"Error in validation: {str(e)}")

# COMMAND ----------

print("="*60)
print("BRONZE LAYER INGESTION SUMMARY")
print("="*60)

try:
    # Final statistics
    bronze_metadata = spark.table(f"{bronze}.bronze_image_metadata")
    bronze_images = spark.table(f"{bronze}.bronze_images") 
    bronze_accident = spark.table(f"{bronze}.bronze_accident")
    
    print(f"📊 Ingestion Results:")
    print(f"   Source metadata file: {metadata_path}")
    print(f"   Source images path: {accidents_path}")
    print(f"   Metadata records: {bronze_metadata.count():,}")
    print(f"   Image files: {bronze_images.count():,}")
    print(f"   Combined records: {bronze_accident.count():,}")
    
    print(f"\n💾 Tables Created:")
    print(f"   • bronze_image_metadata: Metadata only")
    print(f"   • bronze_images: Images with binary content")
    print(f"   • bronze_accident: Combined metadata + images")
    
    print(f"\n🎯 Data Quality:")
    total_size = bronze_images.select(F.sum("file_size")).collect()[0][0] or 0
    avg_size = bronze_images.select(F.avg("file_size")).collect()[0][0] or 0
    print(f"   Total image size: {total_size:,} bytes")
    print(f"   Average image size: {avg_size:,.0f} bytes")
    
    print(f"\n🔗 Next Steps:")
    print(f"   1. ✅ Bronze layer complete")
    print(f"   2. 🤖 Run severity prediction script")
    print(f"   3. 🥈 Create silver tables with ML predictions")
    print(f"   4. 📊 Perform severity analysis")
    print(f"   5. 🔗 Join with claims data")
    
except Exception as e:
    print(f"❌ Error in summary: {str(e)}")

print("="*60)
print("✅ Bronze layer ingestion completed!")
print("="*60)

# COMMAND ----------

print("📋 Data Lineage Information:")
print(f"""
🗂️ SOURCE DATA:
   • Image files: {accidents_path}
   • Metadata: {metadata_path}

🥉 BRONZE TABLES (schema: {bronze}):
   • {bronze}.bronze_image_metadata
     - Purpose: Raw metadata from CSV
     - Source: {metadata_path}
     - Columns: Original metadata + loaded_timestamp, source_file
   
   • {bronze}.bronze_images  
     - Purpose: Raw images with binary content
     - Source: {accidents_path}
     - Columns: path, content, image_name, loaded_timestamp, file_size, source_path
   
   • {bronze}.bronze_accident
     - Purpose: Combined metadata + image references
     - Source: Join of metadata + images
     - Columns: All metadata columns + image path info + timestamps

🔄 PROCESSING NOTES:
   • All tables use Delta format for ACID compliance
   • Tables are optimized for query performance
   • Binary content preserved in bronze_images table
   • Metadata preserved separately for lightweight queries
   • Join keys validated for data quality

🎯 USAGE:
   • Use {bronze}.bronze_image_metadata for metadata-only queries
   • Use {bronze}.bronze_images for image processing workflows
   • Use {bronze}.bronze_accident for combined analysis
   • All tables ready for silver layer processing
""")

print("📋 Lineage documentation complete")
