# Spark-Based ML Prediction Guide for Large Datasets

## Overview

This guide explains how the Smart Claims severity prediction pipeline automatically handles large datasets using Spark-based distributed processing when the number of images exceeds the Pandas threshold.

## Configuration Parameters

### ML Configuration Settings
```python
ml_config = {
    "pandas_threshold": 10000,      # Switch to Spark above this many records
    "batch_size": 1000,             # Batch size for Spark processing (simulated predictions)
    "api_batch_size": 500,          # Smaller batches for API calls (Azure AI Foundry)
    "api_delay_seconds": 0.1,       # Delay between API batches (rate limit protection)
    "enable_caching": True,         # Cache intermediate results
    "max_retries": 3               # Max retries for failed API calls
}
```

## Processing Flow

### 1. Automatic Threshold Detection
```
📊 Images ready for prediction: 13,001
⚠️ Dataset too large (13,001 images) for Pandas conversion
Using Spark-based prediction approach (threshold: 10,000)
```

The system automatically detects when to switch from Pandas to Spark:
- **≤ 10,000 images**: Uses Pandas for faster in-memory processing
- **> 10,000 images**: Uses Spark for distributed processing

### 2. Spark-Based Batch Processing

#### For Simulated Predictions:
```python
# Process in configurable batches
batch_size = ml_config.get("batch_size", 1000)
total_count = images_for_prediction.count()

# Add row numbers for efficient batching
window_spec = Window.orderBy("image_name")
images_with_row_num = images_for_prediction.withColumn("row_num", row_number().over(window_spec))

# Process each batch
for batch_num in range(num_batches):
    start_row = batch_num * batch_size + 1
    end_row = min((batch_num + 1) * batch_size, total_count)
    
    batch_df = (images_with_row_num
               .filter((col("row_num") >= start_row) & (col("row_num") <= end_row))
               .select("image_name")
               .withColumn("severity", severity_udf(col("image_name"))))
```

#### For Azure AI Foundry API Predictions:
```python
# Smaller batches for API calls to respect rate limits
api_batch_size = ml_config.get("api_batch_size", 500)
api_delay = ml_config.get("api_delay_seconds", 0.1)

# Process with error handling and fallbacks
for batch_num in range(num_batches):
    try:
        # API prediction batch
        batch_df = process_api_batch(batch_data)
        time.sleep(api_delay)  # Respect rate limits
    except Exception as batch_error:
        # Fallback to simulated predictions
        batch_df = process_simulated_batch(batch_data)
```

## Performance Optimizations

### 1. Memory Management
- **Batch Processing**: Prevents memory overflow by processing data in chunks
- **Caching Strategy**: Intermediate results are cached and unpersisted appropriately
- **Row Numbering**: Efficient partitioning using Spark's window functions

### 2. API Rate Limiting
- **Configurable Delays**: Adjustable delays between API calls
- **Smaller Batches**: Reduced batch sizes for API-based predictions
- **Automatic Fallback**: Falls back to simulated predictions if API fails

### 3. Error Resilience
- **Batch-Level Error Handling**: Failed batches don't stop entire processing
- **Graceful Degradation**: Falls back to simulated predictions when needed
- **Progress Monitoring**: Real-time feedback on batch processing status

## Monitoring and Logging

### Progress Tracking
```
Processing 13,001 images in batches of 1,000
Processing batch 1/14 (rows 1-1000)
✅ Batch 1 completed: 1,000 predictions
Processing batch 2/14 (rows 1001-2000)
✅ Batch 2 completed: 1,000 predictions
...
```

### Error Handling
```
Processing batch 5/14 (rows 4001-5000)
⚠️ Batch 5 failed, using simulated predictions
✅ Batch 5 completed: 1,000 predictions (fallback)
```

## Performance Characteristics

### Throughput Comparison

| Dataset Size | Processing Method | Estimated Time | Memory Usage |
|--------------|------------------|----------------|--------------|
| < 1,000 images | Pandas | 30 seconds | 500 MB |
| 1,000-10,000 images | Pandas | 2-5 minutes | 2-5 GB |
| 10,000-50,000 images | Spark (Simulated) | 5-15 minutes | Distributed |
| 10,000-50,000 images | Spark (API) | 20-60 minutes | Distributed |
| > 50,000 images | Spark (Optimized) | 30-120 minutes | Distributed |

### Scaling Recommendations

#### Small Datasets (< 5,000 images)
```python
ml_config = {
    "pandas_threshold": 5000,
    "batch_size": 500,
    "enable_caching": False  # Not needed for small datasets
}
```

#### Medium Datasets (5,000-25,000 images)
```python
ml_config = {
    "pandas_threshold": 10000,
    "batch_size": 1000,
    "api_batch_size": 500,
    "enable_caching": True
}
```

#### Large Datasets (> 25,000 images)
```python
ml_config = {
    "pandas_threshold": 10000,
    "batch_size": 2000,        # Larger batches for efficiency
    "api_batch_size": 200,     # Smaller API batches for stability
    "api_delay_seconds": 0.2,  # Longer delays for rate limiting
    "enable_caching": True
}
```

## Troubleshooting

### Common Issues and Solutions

#### 1. Out of Memory Errors
**Problem**: Spark driver or executors run out of memory
```
java.lang.OutOfMemoryError: Java heap space
```

**Solution**: Reduce batch size and enable caching
```python
ml_config = {
    "batch_size": 500,          # Reduce from 1000
    "enable_caching": True,
    "api_batch_size": 200       # Reduce API batch size
}
```

#### 2. API Rate Limiting
**Problem**: Too many API requests
```
Rate limit exceeded (429 error)
```

**Solution**: Increase delays and reduce batch sizes
```python
ml_config = {
    "api_batch_size": 100,      # Smaller batches
    "api_delay_seconds": 0.5,   # Longer delays
    "max_retries": 5           # More retries
}
```

#### 3. Slow Processing
**Problem**: Processing takes too long

**Solution**: Optimize batch sizes and enable parallelization
```python
# Increase Spark parallelism
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# Optimize batch sizes
ml_config = {
    "batch_size": 2000,        # Larger batches for simulated predictions
    "api_batch_size": 500,     # Balanced API batch size
}
```

## Advanced Configuration

### Custom UDF Optimization
```python
# Broadcast variables for large lookup tables
broadcast_model_config = spark.sparkContext.broadcast(ml_config)

def optimized_severity_prediction(image_name):
    config = broadcast_model_config.value
    # Use broadcasted configuration
    return predict_severity(image_name, config)
```

### Dynamic Batch Sizing
```python
def calculate_optimal_batch_size(total_records, available_memory_gb):
    """Calculate optimal batch size based on available resources"""
    if total_records < 5000:
        return min(1000, total_records)
    elif available_memory_gb > 16:
        return 2000
    elif available_memory_gb > 8:
        return 1000
    else:
        return 500

# Apply dynamic sizing
optimal_batch_size = calculate_optimal_batch_size(prediction_count, 16)
ml_config["batch_size"] = optimal_batch_size
```

### Monitoring and Alerting
```python
import time

class ProcessingMonitor:
    def __init__(self):
        self.start_time = time.time()
        self.batch_times = []
    
    def log_batch_completion(self, batch_num, batch_size, success=True):
        current_time = time.time()
        batch_duration = current_time - self.start_time
        self.batch_times.append(batch_duration)
        
        avg_time_per_batch = sum(self.batch_times) / len(self.batch_times)
        estimated_remaining = avg_time_per_batch * (total_batches - batch_num)
        
        print(f"Batch {batch_num}: {batch_size} records in {batch_duration:.1f}s")
        print(f"Estimated remaining time: {estimated_remaining/60:.1f} minutes")

# Usage
monitor = ProcessingMonitor()
for batch_num in range(num_batches):
    # Process batch
    monitor.log_batch_completion(batch_num + 1, batch_size)
```

## Production Recommendations

### 1. Resource Allocation
- **Driver Memory**: Minimum 8GB for large datasets
- **Executor Memory**: 4-8GB per executor
- **Executor Cores**: 2-4 cores per executor for optimal balance

### 2. Configuration Tuning
```python
# Production-optimized configuration
production_config = {
    "pandas_threshold": 15000,     # Higher threshold for production
    "batch_size": 1500,            # Optimized batch size
    "api_batch_size": 300,         # Conservative API batch size
    "api_delay_seconds": 0.15,     # Balanced delay
    "enable_caching": True,
    "max_retries": 5,
    "timeout_seconds": 300         # 5-minute timeout per batch
}
```

### 3. Monitoring Setup
- **Batch completion rates**
- **API error rates and types**
- **Memory usage patterns**
- **Processing time trends**
- **Data quality metrics**

This Spark-based approach ensures your Smart Claims pipeline can handle datasets of any size while maintaining performance, reliability, and cost-effectiveness.
