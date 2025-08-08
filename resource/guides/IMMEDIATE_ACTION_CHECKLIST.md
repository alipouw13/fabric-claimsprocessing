# 🚨 IMMEDIATE ACTION CHECKLIST - Memory Crisis Resolution

## Current Situation
- **Error**: Exit code 137 (container killed due to memory exhaustion)
- **Dataset**: 38,131,615 records causing memory failure
- **Root Cause**: Trying to process entire dataset in memory at once

## IMMEDIATE ACTIONS (Do These Now)

### 1. Restart Your Spark Session ⚡
```python
# In your Fabric notebook, run this first:
spark.stop()
spark = SparkSession.builder.getOrCreate()
spark.catalog.clearCache()
```

### 2. Apply Memory Configuration 🔧
```python
# Run these configurations BEFORE any data processing:
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "268435456")  # 256MB target
spark.conf.set("spark.sql.shuffle.partitions", "400")  # Tune later based on stage metrics
spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")  # 128MB to avoid oversized tasks
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
# Optional (only if supported in Fabric runtime):
# spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
# spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
```

⚠️ Do NOT set `spark.serializer` at runtime in Fabric (immutable). It is already Kryo internally. Verify (read-only):
```python
print(spark.conf.get("spark.serializer", "not exposed"))
```

### 3. Check Dataset Size First 📊
```python
# Before processing, check the size:
bronze_accident = spark.table("bronze_accident")
total_count = bronze_accident.count()
print(f"Total records: {total_count:,}")

if total_count > 10_000_000:
    print("🔴 LARGE DATASET - Use chunked processing")
else:
    print("🟢 Manageable size - Proceed with caution")
```

### 4. Use Chunked Processing for Large Datasets 📦

Adaptive chunk sizing (smaller chunks as volume grows):
```python
# Adaptive chunk sizing heuristic
if total_count <= 10_000_000:
    chunk_size = 1_000_000
elif total_count <= 30_000_000:
    chunk_size = 750_000
elif total_count <= 60_000_000:
    chunk_size = 500_000
else:
    chunk_size = 250_000  # very large -> smaller chunks

num_chunks = (total_count + chunk_size - 1) // chunk_size
print(f"Processing {total_count:,} records in {num_chunks} chunks of ~{chunk_size:,}")
```

Chunk extraction pattern (Spark does not support SQL OFFSET directly – use row_number window):
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

window_spec = Window.orderBy("image_name")  # choose a stable ordering column
bronze_indexed = bronze_accident.withColumn("_rn", row_number().over(window_spec))

for i in range(num_chunks):
    start = i * chunk_size + 1
    end = min((i + 1) * chunk_size, total_count)
    print(f"→ Chunk {i+1}/{num_chunks} rows {start:,}-{end:,}")

    chunk_df = (bronze_indexed
                .filter((col("_rn") >= start) & (col("_rn") <= end))
                .drop("_rn"))

    # process_and_write(chunk_df)  # implement join + enrich + append
    # Recommended: unpersist / clear cache if cached

    del chunk_df
```

Optional: Derive parallelism hint from current shuffle partitions:
```python
shuffle_parts = int(spark.conf.get("spark.sql.shuffle.partitions"))
print("Shuffle partitions:", shuffle_parts)
```

### 5. (Optional) Lightweight Progress Metrics
```python
from time import perf_counter
processed = 0
start_ts = perf_counter()
# inside loop after each chunk write:
# processed += chunk_df.count()
# rate = processed / (perf_counter() - start_ts)
# print(f"Cumulative processed: {processed:,} rows  |  Rate ~ {rate:,.0f} rows/sec")
```

## QUICK VERIFICATION STEPS ✅

### Before You Start:
- [ ] Spark session restarted
- [ ] Memory configurations applied  
- [ ] Dataset size checked
- [ ] Processing strategy selected

### During Processing:
- [ ] Monitor memory usage in Fabric
- [ ] Check for error messages
- [ ] Verify chunks complete successfully
- [ ] Clear cache between chunks

### After Processing:
- [ ] Verify silver table created
- [ ] Check record count matches expectations
- [ ] Validate data quality
- [ ] Test downstream queries

## FALLBACK OPTIONS 🔄

### If Chunked Processing Still Fails:
1. **Reduce chunk size** to 250,000 / 100,000 records
2. **Lower shuffle partitions** if tasks are tiny (e.g. 200)
3. **Increase capacity / concurrency limits**
4. **Use streaming approach** for continuous ingest

### If All Memory Approaches Fail:
1. **Pre-filter** in bronze (drop unused columns early)
2. **Split by date ranges** and process sequentially  
3. **External orchestration** (Data Factory / Pipeline) for staging batches
4. **Incremental pattern** (only new or changed records)

## EMERGENCY CONTACT POINTS 📞

### Technical Issues:
- Check Fabric workspace resource usage
- Review Spark application logs
- Monitor cluster health metrics

### Process Issues:
- Validate bronze table accessibility
- Check predictions table availability
- Verify workspace permissions

## PREVENTION FOR FUTURE 🛡️

### Monitoring Setup:
- Set up alerts for memory usage > 80%
- Monitor dataset growth trends
- Track processing duration

### Architecture Improvements:
- Implement automatic chunking thresholds
- Add memory usage validation
- Create graceful degradation patterns

---

**🎯 PRIMARY GOAL**: Get your silver table created successfully
**⏱️ TIME CRITICAL**: The sooner you apply memory optimizations, the better
**📈 SUCCESS METRIC**: Silver table with 38M+ records without memory errors

**Next Steps After Success:**
1. Validate data quality in silver table
2. Run downstream analytics
3. Implement monitoring for future runs
4. Consider architectural improvements for scalability
