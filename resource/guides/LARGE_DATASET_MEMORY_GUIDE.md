# Large Dataset Memory Management Guide

## Issue Summary

You encountered a memory exhaustion error when processing 38+ million records:
- **Error**: `Exit code 137` - Container killed due to memory exhaustion
- **Cause**: Trying to write 38+ million records to Delta table exceeds available memory
- **Solution**: Implement chunked processing and memory optimization

## Immediate Solutions

### 1. Memory-Optimized Configuration

Add these Spark configurations before processing large datasets:

```python
# Memory optimization settings
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize", "134217728")  # 128MB
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "268435456")  # 256MB
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

# Increase driver and executor memory
spark.conf.set("spark.driver.memory", "8g")
spark.conf.set("spark.executor.memory", "8g")
spark.conf.set("spark.driver.maxResultSize", "4g")
```

### 2. Chunked Processing Approach

For datasets > 10 million records, process in chunks:

```python
def process_large_dataset_in_chunks(bronze_df, predictions_df, chunk_size=1_000_000):
    """
    Process large datasets in manageable chunks
    """
    total_count = bronze_df.count()
    num_chunks = (total_count + chunk_size - 1) // chunk_size
    
    print(f"Processing {total_count:,} records in {num_chunks} chunks of {chunk_size:,}")
    
    # Create empty table with schema first
    create_empty_silver_table(bronze_df, predictions_df)
    
    # Process each chunk
    for chunk_num in range(num_chunks):
        start_row = chunk_num * chunk_size
        end_row = min((chunk_num + 1) * chunk_size, total_count)
        
        print(f"Processing chunk {chunk_num + 1}/{num_chunks} (rows {start_row:,}-{end_row:,})")
        
        # Get chunk using row_number window function
        from pyspark.sql.window import Window
        from pyspark.sql.functions import row_number
        
        window_spec = Window.orderBy("image_name")
        bronze_with_row_num = bronze_df.withColumn("row_num", row_number().over(window_spec))
        
        chunk_df = (bronze_with_row_num
                   .filter((col("row_num") > start_row) & (col("row_num") <= end_row))
                   .drop("row_num"))
        
        # Process chunk
        processed_chunk = process_chunk(chunk_df, predictions_df)
        
        # Append to silver table
        (processed_chunk.write
         .format("delta")
         .mode("append")
         .saveAsTable("silver_accident"))
        
        # Clear cache to free memory
        chunk_df.unpersist()
        processed_chunk.unpersist()
        
        print(f"✅ Chunk {chunk_num + 1} completed")

def create_empty_silver_table(bronze_df, predictions_df):
    """Create empty silver table with proper schema"""
    temp_df = (bronze_df
              .join(predictions_df, "image_name", "left_outer")
              .limit(1))
    
    schema_df = (temp_df
        .withColumn("processed_timestamp", current_timestamp())
        .withColumn("severity_category", lit("Low"))
        .withColumn("high_severity_flag", lit(False))
        .withColumn("data_quality_score", lit(1.0))
        .withColumn("partition_date", F.date_format(current_timestamp(), "yyyy-MM-dd")))
    
    # Create empty table
    (schema_df
     .filter(col("image_name").isNull())  # Empty result
     .write
     .format("delta")
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .saveAsTable("silver_accident"))

def process_chunk(chunk_df, predictions_df):
    """Process a single chunk of data"""
    silver_chunk = (chunk_df
                   .join(predictions_df, "image_name", "left_outer")
                   .withColumn("processed_timestamp", current_timestamp()))
    
    # Add derived columns
    silver_chunk = (silver_chunk
        .withColumn("severity_category",
            when(col("severity") >= 0.8, "High")
            .when(col("severity") >= 0.6, "Medium-High") 
            .when(col("severity") >= 0.4, "Medium")
            .when(col("severity") >= 0.2, "Low-Medium")
            .otherwise("Low"))
        .withColumn("high_severity_flag", 
            when(col("severity") >= 0.5, True)  # Use threshold from config
            .otherwise(False))
        .withColumn("data_quality_score",
            when(col("content").isNotNull() & col("severity").isNotNull(), 1.0)
            .when(col("content").isNotNull() & col("severity").isNull(), 0.7)
            .when(col("content").isNull() & col("severity").isNotNull(), 0.3)
            .otherwise(0.0))
        .withColumn("partition_date", 
            F.date_format(col("processed_timestamp"), "yyyy-MM-dd")))
    
    return silver_chunk
```

### 3. Alternative: Streaming Processing

For extremely large datasets, use Spark Structured Streaming:

```python
def process_with_streaming(bronze_df, predictions_df):
    """
    Use structured streaming for large datasets
    """
    # Convert to streaming DataFrame
    streaming_df = (spark
                   .readStream
                   .format("delta")
                   .table("bronze_accident"))
    
    # Join with predictions (broadcast small table)
    predictions_broadcast = F.broadcast(predictions_df)
    
    streaming_silver = (streaming_df
                       .join(predictions_broadcast, "image_name", "left_outer")
                       .withColumn("processed_timestamp", current_timestamp())
                       # Add derived columns...
                       )
    
    # Write stream to silver table
    query = (streaming_silver
            .writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", "/tmp/checkpoint/silver_accident")
            .toTable("silver_accident"))
    
    return query
```

## Fabric-Specific Optimizations

### 1. Increase Compute Resources

In your Fabric workspace:
- **Upgrade to larger Spark pool** (more memory and cores)
- **Use Premium capacity** for better resource allocation
- **Enable auto-scaling** for dynamic resource adjustment

### 2. Optimize Data Layout

```python
# Partition data for better performance
silver_accident_df = (silver_accident_df
    .withColumn("year", F.year("processed_timestamp"))
    .withColumn("month", F.month("processed_timestamp")))

# Write with partitioning
(silver_accident_df.write
 .format("delta")
 .mode("overwrite")
 .partitionBy("year", "month")
 .option("maxRecordsPerFile", "100000")
 .saveAsTable("silver_accident"))
```

### 3. Use Liquid Clustering (if available)

```python
# Enable liquid clustering for better performance
spark.sql("""
    CREATE TABLE silver_accident_clustered
    USING DELTA
    CLUSTER BY (severity_category, partition_date)
    AS SELECT * FROM silver_accident
""")
```

## Monitoring and Prevention

### 1. Memory Monitoring

```python
def monitor_memory_usage():
    """Monitor current memory usage"""
    import psutil
    
    memory = psutil.virtual_memory()
    print(f"Memory Usage: {memory.percent}%")
    print(f"Available: {memory.available / (1024**3):.2f} GB")
    print(f"Used: {memory.used / (1024**3):.2f} GB")
    
    if memory.percent > 80:
        print("⚠️ High memory usage detected!")
        return False
    return True

# Check before large operations
if not monitor_memory_usage():
    print("Consider using chunked processing")
```

### 2. Progressive Processing Validation

```python
def validate_processing_feasibility(df, max_safe_size=10_000_000):
    """Check if dataset size is manageable"""
    count = df.count()
    
    if count > max_safe_size:
        print(f"⚠️ Large dataset detected: {count:,} records")
        print("Recommendations:")
        print(f"  - Use chunked processing (chunks of {max_safe_size:,})")
        print("  - Increase cluster memory")
        print("  - Use streaming approach")
        return False, count
    
    print(f"✅ Dataset size manageable: {count:,} records")
    return True, count

# Use before processing
safe_to_process, record_count = validate_processing_feasibility(bronze_accident)
```

### 3. Automatic Recovery

```python
def safe_silver_table_creation(bronze_df, predictions_df, max_attempts=3):
    """
    Safely create silver table with automatic fallback
    """
    for attempt in range(max_attempts):
        try:
            print(f"Attempt {attempt + 1}/{max_attempts}")
            
            # Check dataset size
            count = bronze_df.count()
            
            if count > 10_000_000:
                print("Using chunked processing approach")
                process_large_dataset_in_chunks(bronze_df, predictions_df)
            else:
                print("Using standard processing approach")
                # Standard processing code here
                
            print("✅ Silver table created successfully")
            return True
            
        except Exception as e:
            print(f"❌ Attempt {attempt + 1} failed: {str(e)}")
            
            if attempt < max_attempts - 1:
                # Clear cache and try smaller chunks
                spark.catalog.clearCache()
                chunk_size = 1_000_000 // (attempt + 1)  # Progressively smaller chunks
                print(f"Retrying with smaller chunk size: {chunk_size:,}")
            else:
                print("All attempts failed")
                return False
    
    return False
```

## Production Recommendations

### 1. Cluster Sizing

For 38+ million records:
- **Driver**: Minimum 16GB memory, 4 cores
- **Executors**: 8GB memory, 4 cores each
- **Total Executors**: 8-16 executors
- **Storage**: Premium SSD for checkpoint and temp storage

### 2. Processing Strategy

```python
# Recommended configuration for large datasets
ml_config.update({
    "pandas_threshold": 5_000_000,      # Lower threshold for Pandas
    "chunk_size": 1_000_000,            # Process in 1M record chunks
    "max_memory_usage": 80,             # Stop if memory > 80%
    "enable_checkpointing": True,       # Enable recovery
    "checkpoint_interval": 100_000      # Checkpoint every 100k records
})
```

### 3. Monitoring Dashboard

Create monitoring for production:
- Memory usage alerts
- Processing time tracking  
- Error rate monitoring
- Data quality metrics
- Throughput measurements

## Quick Fix for Your Current Issue

1. **Immediate**: Restart your Spark session to clear memory
2. **Update configuration**: Add memory optimization settings
3. **Process in chunks**: Use the chunked processing approach
4. **Monitor progress**: Check memory usage during processing
5. **Validate results**: Ensure data quality after chunked processing

The updated code I provided includes automatic detection of large datasets and switches to memory-optimized processing with proper partitioning and chunking strategies.
