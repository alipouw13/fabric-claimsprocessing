# Microsoft Fabric notebook source
# This notebook has been adapted from the original Databricks version
# Available at https://github.com/databricks-industry-solutions/smart-claims

# COMMAND ----------

# Install geopy for geocoding
# NOTE: Attach your lakehouse to this notebook to ensure no errors

import subprocess
import sys

def install_package(package_name):
    """Install package if not already available"""
    try:
        __import__(package_name)
        print(f"✅ {package_name} is already installed")
    except ImportError:
        print(f"Installing {package_name}...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", package_name])
        print(f"✅ {package_name} installed successfully")

# Install required packages
install_package("geopy")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Libraries and Setup

# COMMAND ----------

import geopy
import pandas as pd
import logging
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import time

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

print("✅ Libraries imported successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration and Helper Functions

# COMMAND ----------

def get_lat_long(df_row, geolocator, address_field="address", lat_field="latitude", long_field="longitude", retry_count=3):
    """
    Get latitude and longitude for an address with retry logic
    """
    address = df_row[address_field]
    
    for attempt in range(retry_count):
        try:
            if pd.isna(address) or address.strip() == "":
                df_row[lat_field] = None
                df_row[long_field] = None
                return df_row
            
            # Geocode the address
            location = geolocator.geocode(address, timeout=10)
            
            if location:
                df_row[lat_field] = location.latitude
                df_row[long_field] = location.longitude
                logger.debug(f"Successfully geocoded: {address}")
            else:
                df_row[lat_field] = None
                df_row[long_field] = None
                logger.warning(f"No coordinates found for: {address}")
            
            return df_row
            
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed for {address}: {str(e)}")
            if attempt < retry_count - 1:
                time.sleep(1)  # Wait before retry
            else:
                # Final attempt failed
                df_row[lat_field] = None
                df_row[long_field] = None
                logger.error(f"Failed to geocode after {retry_count} attempts: {address}")
    
    return df_row

def geocode_addresses_batch(addresses_df, batch_size=50):
    """
    Geocode addresses in batches to avoid rate limiting
    """
    # Initialize geolocator with a more descriptive user agent
    geolocator = geopy.Nominatim(
        user_agent="fabric_smart_claims_geocoder_v1.0", 
        timeout=10
    )
    
    total_addresses = len(addresses_df)
    logger.info(f"Starting geocoding for {total_addresses} unique addresses...")
    
    # Initialize result columns
    addresses_df["latitude"] = None
    addresses_df["longitude"] = None
    
    # Process in batches
    for i in range(0, total_addresses, batch_size):
        batch_end = min(i + batch_size, total_addresses)
        logger.info(f"Processing batch {i//batch_size + 1}: addresses {i+1} to {batch_end}")
        
        # Apply geocoding to batch
        batch_df = addresses_df.iloc[i:batch_end].copy()
        batch_df = batch_df.apply(
            lambda row: get_lat_long(row, geolocator), 
            axis=1
        )
        
        # Update main dataframe
        addresses_df.iloc[i:batch_end] = batch_df
        
        # Rate limiting - pause between batches
        if batch_end < total_addresses:
            logger.info("Pausing between batches to respect rate limits...")
            time.sleep(2)
    
    # Log results
    successful_geocodes = addresses_df.dropna(subset=['latitude', 'longitude']).shape[0]
    logger.info(f"Geocoding complete: {successful_geocodes}/{total_addresses} addresses successfully geocoded")
    
    return addresses_df

# COMMAND ----------

# Define silver schema
silver = "silver"
try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {silver}")
except Exception as e:
    logger.warning(f"Schema creation warning for {silver}: {e}")

# COMMAND ----------

# Load the silver claim policy table
logger.info("Loading silver_claim_policy table...")

try:
    policy_claim_df = spark.table(f"{silver}.silver_claim_policy")
    record_count = policy_claim_df.count()
    logger.info(f"Loaded {record_count:,} records from silver_claim_policy")
    
    # Show sample data
    print("\nSample data from silver_claim_policy:")
    display(policy_claim_df.limit(5))
    
    # Check for required columns
    columns = policy_claim_df.columns
    required_cols = ["BOROUGH", "ZIP_CODE"]
    missing_cols = [col for col in required_cols if col not in columns]
    
    if missing_cols:
        logger.error(f"Missing required columns: {missing_cols}")
        logger.info(f"Available columns: {columns}")
        raise ValueError(f"Required columns missing: {missing_cols}")
    else:
        logger.info("✅ All required columns found")
        
except Exception as e:
    logger.error(f"Error loading silver_claim_policy table: {str(e)}")
    raise


# COMMAND ----------

# Create address field by combining BOROUGH and ZIP_CODE
logger.info("Creating address field from BOROUGH and ZIP_CODE...")

policy_claim_with_address = policy_claim_df.withColumn(
    "address", 
    F.concat(
        F.col("BOROUGH"), 
        F.lit(", "), 
        F.col("ZIP_CODE").cast("int").cast("string")
    )
)

# Filter out null addresses
valid_address_count = policy_claim_with_address.filter(F.col("address").isNotNull()).count()
total_count = policy_claim_with_address.count()

logger.info(f"Address field created: {valid_address_count:,} valid addresses out of {total_count:,} records")

# Show sample addresses
print("\nSample data with address field:")
display(policy_claim_with_address.select("BOROUGH", "ZIP_CODE", "address").limit(10))

# COMMAND ----------

# Convert to Pandas for geocoding (only unique addresses to minimize API calls)
logger.info("Extracting unique addresses for geocoding...")

policy_claim_with_address_pd = (policy_claim_with_address
                               .filter(F.col("address").isNotNull())
                               .select("address")
                               .distinct()
                               .toPandas())

unique_addresses_count = len(policy_claim_with_address_pd)
logger.info(f"Found {unique_addresses_count:,} unique addresses to geocode")

# Create DataFrame with unique addresses
unique_address_df = pd.DataFrame()
unique_address_df["address"] = policy_claim_with_address_pd["address"].unique()

print(f"\nFirst 10 unique addresses to geocode:")
display(unique_address_df.head(10))

# COMMAND ----------

# Perform geocoding in batches
logger.info("Starting geocoding process...")

try:
    # Geocode all unique addresses
    geocoded_addresses = geocode_addresses_batch(unique_address_df, batch_size=25)
    
    # Show results
    successful_geocodes = geocoded_addresses.dropna(subset=['latitude', 'longitude']).shape[0]
    total_addresses = len(geocoded_addresses)
    
    print(f"\n📊 Geocoding Results:")
    print(f"Total addresses: {total_addresses:,}")
    print(f"Successfully geocoded: {successful_geocodes:,}")
    print(f"Failed geocodes: {total_addresses - successful_geocodes:,}")
    print(f"Success rate: {(successful_geocodes/total_addresses)*100:.1f}%")
    
    # Display sample results
    print("\nSample geocoded addresses:")
    display(geocoded_addresses.head(10))
    
except Exception as e:
    logger.error(f"Error during geocoding: {str(e)}")
    raise

# COMMAND ----------

# Convert geocoded results back to Spark DataFrame
logger.info("Converting geocoded results back to Spark DataFrame...")

try:
    # Define schema for the geocoded data
    geocoded_schema = StructType([
        StructField("address", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True)
    ])
    
    # Create Spark DataFrame
    unique_address_spark_df = spark.createDataFrame(geocoded_addresses, schema=geocoded_schema)
    
    logger.info("✅ Successfully converted to Spark DataFrame")
    
    # Show schema and sample data
    print("\nGeocoded data schema:")
    unique_address_spark_df.printSchema()
    
    print("\nSample geocoded data:")
    display(unique_address_spark_df.limit(10))
    
except Exception as e:
    logger.error(f"Error converting to Spark DataFrame: {str(e)}")
    raise

# COMMAND ----------

# Join geocoded coordinates back to the main dataset
logger.info("Joining geocoded coordinates back to main dataset...")

try:
    policy_claim_lat_long = policy_claim_with_address.join(
        unique_address_spark_df, 
        on="address", 
        how="left"  # Use left join to keep all records
    )
    
    # Count results
    total_records = policy_claim_lat_long.count()
    geocoded_records = policy_claim_lat_long.filter(
        F.col("latitude").isNotNull() & F.col("longitude").isNotNull()
    ).count()
    
    logger.info(f"Join completed: {geocoded_records:,} out of {total_records:,} records have coordinates")
    
    # Show sample of final data
    print("\nSample of final dataset with coordinates:")
    display(policy_claim_lat_long.select(
        "policy_no", "claim_no", "address", "latitude", "longitude"
    ).limit(10))
    
except Exception as e:
    logger.error(f"Error joining data: {str(e)}")
    raise

# COMMAND ----------

# Save the enhanced dataset with location data
logger.info("Saving enhanced dataset to silver_claim_policy_location table...")

try:
    # Add metadata
    final_df = policy_claim_lat_long.withColumn(
        "location_processed_timestamp", 
        F.current_timestamp()
    )
    
    # Write to Delta table
    (final_df.write
     .format("delta")
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .saveAsTable(f"{silver}.silver_claim_policy_location"))
    spark.sql(f"OPTIMIZE {silver}.silver_claim_policy_location")
    saved_count = spark.table(f"{silver}.silver_claim_policy_location").count()
    
    logger.info(f"✅ Successfully saved {saved_count:,} records to silver_claim_policy_location")
    
except Exception as e:
    logger.error(f"Error saving table: {str(e)}")
    raise

# COMMAND ----------

# Final summary
print("="*60)
print("LOCATION ENHANCEMENT PIPELINE SUMMARY")
print("="*60)

try:
    # Get table statistics
    location_table = spark.table(f"{silver}.silver_claim_policy_location")
    total_records = location_table.count()
    
    # Count records with coordinates
    with_coordinates = location_table.filter(
        F.col("latitude").isNotNull() & F.col("longitude").isNotNull()
    ).count()
    
    # Count unique locations
    unique_locations = location_table.filter(
        F.col("latitude").isNotNull() & F.col("longitude").isNotNull()
    ).select("latitude", "longitude").distinct().count()
    
    print(f"📊 Final Results:")
    print(f"Total records: {total_records:,}")
    print(f"Records with coordinates: {with_coordinates:,}")
    print(f"Records without coordinates: {(total_records - with_coordinates):,}")
    print(f"Unique locations: {unique_locations:,}")
    print(f"Geocoding success rate: {(with_coordinates/total_records)*100:.1f}%")
    
    print(f"\n✅ Location enhancement pipeline completed successfully!")
    
    # Sample of final data
    print(f"\nSample records with location data:")
    display(location_table.filter(
        F.col("latitude").isNotNull()
    ).select(
        "policy_no", "claim_no", "BOROUGH", "ZIP_CODE", "address", "latitude", "longitude"
    ).limit(5))
    
except Exception as e:
    print(f"❌ Error in final validation: {str(e)}")

print("="*60)

# COMMAND ----------

# Perform data quality checks on the location data
logger.info("Performing data quality checks...")

try:
    location_table = spark.table(f"{silver}.silver_claim_policy_location")
    
    # Check for valid coordinate ranges
    invalid_lat = location_table.filter(
        (F.col("latitude") < -90) | (F.col("latitude") > 90)
    ).count()
    
    invalid_lng = location_table.filter(
        (F.col("longitude") < -180) | (F.col("longitude") > 180)
    ).count()
    
    # Check for coordinates at (0,0) which might indicate geocoding errors
    zero_coordinates = location_table.filter(
        (F.col("latitude") == 0) & (F.col("longitude") == 0)
    ).count()
    
    print(f"\n🔍 Data Quality Results:")
    print(f"Invalid latitudes: {invalid_lat:,}")
    print(f"Invalid longitudes: {invalid_lng:,}")
    print(f"Zero coordinates (potential errors): {zero_coordinates:,}")
    
    if invalid_lat == 0 and invalid_lng == 0:
        print("✅ All coordinates are within valid ranges")
    else:
        print("⚠️  Some coordinates are outside valid ranges")
        
except Exception as e:
    logger.error(f"Error in data quality checks: {str(e)}")
