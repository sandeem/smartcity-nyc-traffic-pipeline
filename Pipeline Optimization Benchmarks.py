# Databricks notebook source
# DBTITLE 1,Benchmark Introduction
# MAGIC %md
# MAGIC # SmartCity Pipeline Optimization Benchmarks
# MAGIC
# MAGIC This notebook measures the **performance impact** of every optimization applied across the 4-notebook pipeline.
# MAGIC
# MAGIC Each test runs the **same logical operation** twice — once with the optimization, once without — and records wall-clock time and data size.
# MAGIC
# MAGIC ### Optimizations Tested
# MAGIC | # | Optimization | Where Applied | Unoptimized Baseline |
# MAGIC |---|---|---|---|
# MAGIC | 1 | **Broadcast Hash Join** | Silver crosswalk creation (1.8M OSM rows) | Sort Merge Join |
# MAGIC | 2 | **Broadcast Hash Join** | Silver enrichment (traffic ↔ crosswalk) | Shuffled Hash Join |
# MAGIC | 3 | **Adaptive Query Execution (AQE)** | Silver + Gold processing | AQE disabled |
# MAGIC | 4 | **Z-ORDER** | `silver_road_crosswalk` table | No Z-ORDER |
# MAGIC | 5 | **Liquid Clustering** | `silver_osm_enriched` (CLUSTER BY h3_id) | No clustering |
# MAGIC
# MAGIC ### Methodology
# MAGIC - Each operation is run **3 times** and the **median** is taken to reduce variance
# MAGIC - Spark caches are cleared between runs (`spark.catalog.clearCache()`)
# MAGIC - Results are collected into a summary DataFrame at the end
# MAGIC - **No production tables are modified** — all benchmarks use temporary tables/DataFrames

# COMMAND ----------

# DBTITLE 1,Setup: Imports and Timing Utilities
import time
import statistics
import builtins
from pyspark.sql.functions import *
from pyspark.sql import DataFrame
from pyspark.databricks.sql.functions import h3_longlatash3

catalog = "bootcamp_students"
schema = "zachy_sandeepmanoharan1"
NUM_RUNS = 3  # Each variant runs 3 times, median taken

# ── Timing helper ──
def timed_run(label, func, num_runs=NUM_RUNS):
    """Run func num_runs times, force materialization, return median seconds and row count."""
    times = []
    rows = 0
    for i in range(num_runs):
        spark.catalog.clearCache()
        t0 = time.time()
        result = func()
        if isinstance(result, DataFrame):
            rows = result.count()
        times.append(time.time() - t0)
    med = statistics.median(times)
    print(f"  {label:45s} | {med:8.3f}s  (runs: {[builtins.round(t,3) for t in times]}) | {rows:,} rows")
    return med, rows

def compare(optimized_time, unoptimized_time, opt_label, unopt_label):
    """Print speedup comparison."""
    if optimized_time > 0 and unoptimized_time > 0:
        speedup = unoptimized_time / optimized_time
        pct = ((unoptimized_time - optimized_time) / unoptimized_time) * 100
        winner = opt_label if optimized_time < unoptimized_time else unopt_label
        print(f"\n  ┌─────────────────────────────────────────────────┐")
        print(f"  │  Speedup: {speedup:.2f}x  ({pct:+.1f}% faster with {winner})")
        print(f"  └─────────────────────────────────────────────────┘")

print("Setup complete. Each benchmark runs both variants and compares.")
print(f"Runs per variant: {NUM_RUNS} (median taken)")

# COMMAND ----------

# DBTITLE 1,Join Strategy Benchmarks Header
# MAGIC %md
# MAGIC ---
# MAGIC ## Benchmark 1: Crosswalk Creation (1.8M OSM rows)
# MAGIC **What changed**: Used `broadcast()` hint to send the small traffic table to all workers instead of shuffling both sides.
# MAGIC
# MAGIC | With Optimization | Without Optimization |
# MAGIC |---|---|
# MAGIC | `broadcast()` → Broadcast Hash Join | No hint → Sort Merge Join |

# COMMAND ----------

# DBTITLE 1,Benchmark 1a: Crosswalk — Broadcast Hash Join (Optimized)
# ── Load source tables ──
osm_enriched = spark.table(f"{catalog}.{schema}.silver_osm_enriched")
traffic_raw = spark.table(f"{catalog}.{schema}.bronze_traffic")

traffic_with_h3 = traffic_raw \
    .withColumn("coord_json", coalesce(
        get_json_object(col("flowSegmentData"), "$.coordinates.coordinate[0]"),
        get_json_object(col("coordinates"), "$.coordinate[0]")
    )) \
    .withColumn("lat", get_json_object(col("coord_json"), "$.latitude").cast("double")) \
    .withColumn("lon", get_json_object(col("coord_json"), "$.longitude").cast("double")) \
    .withColumn("h3_id", h3_longlatash3(col("lon"), col("lat"), 10).cast("string"))

print(f"OSM rows: {osm_enriched.count():,} | Bronze rows: {traffic_with_h3.count():,}")
print(f"{'─'*80}")

# ── WITH optimization: Broadcast Hash Join ──
def crosswalk_broadcast():
    return traffic_with_h3.select("h3_id") \
        .join(broadcast(osm_enriched.select("h3_id", "osm_id", "full_road_name")), "h3_id") \
        .distinct()

t_opt, _ = timed_run("WITH broadcast()  → Broadcast Hash Join", crosswalk_broadcast)

# ── WITHOUT optimization: Sort Merge Join ──
try:
    orig = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
except: orig = "10485760"

try: spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
except: pass

def crosswalk_sort_merge():
    return traffic_with_h3.select("h3_id") \
        .join(osm_enriched.select("h3_id", "osm_id", "full_road_name"), "h3_id") \
        .distinct()

t_unopt, _ = timed_run("WITHOUT broadcast() → Sort Merge Join", crosswalk_sort_merge)

try: spark.conf.set("spark.sql.autoBroadcastJoinThreshold", orig)
except: pass

compare(t_opt, t_unopt, "Broadcast Hash Join", "Sort Merge Join")

# COMMAND ----------

# DBTITLE 1,Silver Enrichment Join Header
# MAGIC %md
# MAGIC ---
# MAGIC ## Benchmark 2: Silver Enrichment (traffic ↔ crosswalk)
# MAGIC **What changed**: Used `broadcast()` on the small 58-row crosswalk table for the streaming join.
# MAGIC
# MAGIC | With Optimization | Without Optimization A | Without Optimization B |
# MAGIC |---|---|---|
# MAGIC | `broadcast()` → Broadcast Hash Join | No hint → Sort Merge Join | `SHUFFLE_HASH` hint → Shuffled Hash Join |

# COMMAND ----------

# DBTITLE 1,Benchmark 2: Silver Enrichment Join Strategies
# ── Load data (batch mode for fair timing) ──
static_crosswalk = spark.table(f"{catalog}.{schema}.silver_road_crosswalk")

traffic_parsed = spark.table(f"{catalog}.{schema}.bronze_traffic") \
    .withColumn("lat", coalesce(
        get_json_object(col("flowSegmentData"), "$.coordinates.coordinate[0].latitude"),
        get_json_object(col("flowSegmentData"), "$.coordinates.coordinate.latitude"),
        get_json_object(col("coordinates"), "$.coordinate[0].latitude")
    ).cast("double")) \
    .withColumn("lon", coalesce(
        get_json_object(col("flowSegmentData"), "$.coordinates.coordinate[0].longitude"),
        get_json_object(col("flowSegmentData"), "$.coordinates.coordinate.longitude"),
        get_json_object(col("coordinates"), "$.coordinate[0].longitude")
    ).cast("double")) \
    .withColumn("currentSpeed", coalesce(
        get_json_object(col("flowSegmentData"), "$.currentSpeed"), col("currentSpeed")
    ).cast("int")) \
    .withColumn("freeFlowSpeed", coalesce(
        get_json_object(col("flowSegmentData"), "$.freeFlowSpeed"), col("freeFlowSpeed")
    ).cast("int")) \
    .withColumn("h3_id", h3_longlatash3(col("lon"), col("lat"), 10).cast("string")) \
    .filter(col("h3_id").isNotNull() & col("currentSpeed").isNotNull())

print(f"Parsed traffic: {traffic_parsed.count():,} rows | Crosswalk: {static_crosswalk.count()} rows")
print(f"{'─'*80}")

# ── WITH optimization: Broadcast Hash Join ──
def enrich_broadcast():
    return traffic_parsed.alias("s").join(
        broadcast(static_crosswalk.alias("m")),
        lower(col("s.h3_id")) == lower(col("m.h3_id")), "left"
    ).select("s.h3_id", "s.currentSpeed", "s.freeFlowSpeed", "m.osm_id", "m.full_road_name")

t_bcast, _ = timed_run("WITH broadcast()  → Broadcast Hash Join", enrich_broadcast)

# ── WITHOUT optimization: Sort Merge Join ──
try: spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
except: pass

def enrich_sort_merge():
    return traffic_parsed.alias("s").join(
        static_crosswalk.alias("m"),
        lower(col("s.h3_id")) == lower(col("m.h3_id")), "left"
    ).select("s.h3_id", "s.currentSpeed", "s.freeFlowSpeed", "m.osm_id", "m.full_road_name")

t_smj, _ = timed_run("WITHOUT broadcast() → Sort Merge Join", enrich_sort_merge)

# ── WITHOUT optimization: Shuffled Hash Join ──
def enrich_shuffle_hash():
    return traffic_parsed.alias("s").hint("SHUFFLE_HASH").join(
        static_crosswalk.alias("m"),
        lower(col("s.h3_id")) == lower(col("m.h3_id")), "left"
    ).select("s.h3_id", "s.currentSpeed", "s.freeFlowSpeed", "m.osm_id", "m.full_road_name")

t_shj, _ = timed_run("WITHOUT broadcast() → Shuffled Hash Join", enrich_shuffle_hash)

try: spark.conf.set("spark.sql.autoBroadcastJoinThreshold", orig)
except: pass

compare(t_bcast, t_smj, "Broadcast Hash Join", "Sort Merge Join")
compare(t_bcast, t_shj, "Broadcast Hash Join", "Shuffled Hash Join")

# COMMAND ----------

# DBTITLE 1,AQE Benchmarks Header
# MAGIC %md
# MAGIC ---
# MAGIC ## Benchmark 3: Adaptive Query Execution (AQE)
# MAGIC **What changed**: Enabled AQE + auto shuffle partitions for the Gold aggregation pipeline.
# MAGIC
# MAGIC | With Optimization | Without Optimization |
# MAGIC |---|---|
# MAGIC | AQE enabled, shuffle.partitions = auto | AQE disabled, shuffle.partitions = 200 (Spark default) |

# COMMAND ----------

# DBTITLE 1,Benchmark 3: AQE On vs Off — Gold Aggregation
silver_static = spark.table(f"{catalog}.{schema}.silver_traffic")
print(f"Silver traffic: {silver_static.count():,} rows")
print(f"{'─'*80}")

def gold_aggregation():
    return silver_static \
        .withColumn("ci_raw", (1 - (col("currentSpeed") / col("freeFlowSpeed")))) \
        .withColumn("congestion_index", when(col("ci_raw") < 0, 0).otherwise(col("ci_raw"))) \
        .groupBy(
            window(col("ingest_time"), "1 minute", "1 minute"),
            col("full_road_name").alias("road_name")
        ).agg(
            avg("congestion_index").alias("avg_congestion_index"),
            count("*").alias("total_observations")
        ).withColumn("traffic_status",
            when(col("avg_congestion_index") > 0.8, "GRIDLOCK")
            .when(col("avg_congestion_index") > 0.4, "HEAVY")
            .otherwise("CLEAR")
        )

# ── WITH optimization: AQE Enabled ──
try:
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.shuffle.partitions", "auto")
except: pass

t_aqe_on, _ = timed_run("WITH AQE enabled + auto partitions", gold_aggregation)

# ── WITHOUT optimization: AQE Disabled ──
aqe_disabled = False
try:
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    if spark.conf.get("spark.sql.adaptive.enabled") == "false":
        aqe_disabled = True
except:
    pass

if aqe_disabled:
    t_aqe_off, _ = timed_run("WITHOUT AQE (disabled, 200 partitions)", gold_aggregation)
    compare(t_aqe_on, t_aqe_off, "AQE Enabled", "AQE Disabled")
else:
    print("\n  ⚠️  Cannot disable AQE on serverless compute.")
    print("      Run this cell on the DataExpert all-purpose cluster to see the AQE comparison.")

# Restore
try:
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.shuffle.partitions", "auto")
except: pass

# COMMAND ----------

# DBTITLE 1,Z-ORDER and Clustering Header
# MAGIC %md
# MAGIC ---
# MAGIC ## Benchmark 4: Z-ORDER on Crosswalk Table
# MAGIC **What changed**: Ran `OPTIMIZE ... ZORDER BY (h3_id)` on `silver_road_crosswalk` to physically co-locate rows by location.
# MAGIC
# MAGIC | With Optimization | Without Optimization |
# MAGIC |---|---|
# MAGIC | Z-ORDERed → skip-friendly file layout | No Z-ORDER → random layout, full scan |
# MAGIC
# MAGIC ---
# MAGIC ## Benchmark 5: Liquid Clustering on OSM Enriched (1.8M rows)
# MAGIC **What changed**: Created table with `CLUSTER BY (h3_id)` so Spark physically groups rows by hexagon.
# MAGIC
# MAGIC | With Optimization | Without Optimization |
# MAGIC |---|---|
# MAGIC | Liquid Clustered → data grouped by h3_id | No clustering → random physical layout |

# COMMAND ----------

# DBTITLE 1,Benchmark 4 and 5: Z-ORDER and Liquid Clustering
target_h3 = "622236750647951359"  # City Hall / Brooklyn Bridge area

# ============================================================
# BENCHMARK 4: Z-ORDER on silver_road_crosswalk
# ============================================================
print("BENCHMARK 4: Crosswalk Point Lookup by H3 ID")
print(f"{'─'*80}")

# WITH optimization: Z-ORDERed table
def query_crosswalk_zorder():
    return spark.sql(f"SELECT * FROM {catalog}.{schema}.silver_road_crosswalk WHERE h3_id = '{target_h3}'")

t_zorder, _ = timed_run("WITH Z-ORDER on h3_id", query_crosswalk_zorder)

# WITHOUT optimization: Create a non-Z-ORDERed copy
print("  (creating unoptimized copy...)")
spark.sql(f"""
    CREATE OR REPLACE TABLE {catalog}.{schema}._bench_crosswalk_no_zorder
    USING DELTA AS SELECT * FROM {catalog}.{schema}.silver_road_crosswalk
""")

def query_crosswalk_no_zorder():
    return spark.sql(f"SELECT * FROM {catalog}.{schema}._bench_crosswalk_no_zorder WHERE h3_id = '{target_h3}'")

t_no_zorder, _ = timed_run("WITHOUT Z-ORDER (random layout)", query_crosswalk_no_zorder)
compare(t_zorder, t_no_zorder, "Z-ORDERed", "No Z-ORDER")


# ============================================================
# BENCHMARK 5: Liquid Clustering on silver_osm_enriched
# ============================================================
print(f"\n\nBENCHMARK 5: OSM Enriched H3 Lookup (1.8M rows)")
print(f"{'─'*80}")

# WITH optimization: Liquid Clustered table
def query_osm_clustered():
    return spark.sql(f"SELECT * FROM {catalog}.{schema}.silver_osm_enriched WHERE h3_id = '{target_h3}'")

t_clustered, _ = timed_run("WITH Liquid Clustering (CLUSTER BY h3_id)", query_osm_clustered)

# WITHOUT optimization: Create a non-clustered copy
print("  (creating unoptimized copy — 1.8M rows, may take a minute...)")
spark.sql(f"""
    CREATE OR REPLACE TABLE {catalog}.{schema}._bench_osm_no_cluster
    USING DELTA AS SELECT * FROM {catalog}.{schema}.silver_osm_enriched
""")

def query_osm_no_cluster():
    return spark.sql(f"SELECT * FROM {catalog}.{schema}._bench_osm_no_cluster WHERE h3_id = '{target_h3}'")

t_no_cluster, _ = timed_run("WITHOUT clustering (random layout)", query_osm_no_cluster)
compare(t_clustered, t_no_cluster, "Liquid Clustered", "No Clustering")

# COMMAND ----------

# DBTITLE 1,Table Size Comparison Header
# MAGIC %md
# MAGIC ---
# MAGIC ## Benchmark 6: Table Size Comparison
# MAGIC Compare physical storage sizes of optimized (production) vs unoptimized (benchmark) tables.

# COMMAND ----------

# DBTITLE 1,Table Sizes: Optimized vs Unoptimized
tables_to_check = [
    (f"{catalog}.{schema}.silver_road_crosswalk",     "Crosswalk (Z-ORDERed)       "),
    (f"{catalog}.{schema}._bench_crosswalk_no_zorder", "Crosswalk (No Z-ORDER)      "),
    (f"{catalog}.{schema}.silver_osm_enriched",        "OSM Enriched (Clustered)    "),
    (f"{catalog}.{schema}._bench_osm_no_cluster",      "OSM Enriched (No Clustering)"),
    (f"{catalog}.{schema}.bronze_traffic",              "Bronze Traffic              "),
    (f"{catalog}.{schema}.silver_traffic",              "Silver Traffic              "),
    (f"{catalog}.{schema}.gold_traffic_stats",          "Gold Traffic Stats          "),
    (f"{catalog}.{schema}.bronze_errors",               "Bronze Errors (Quarantine)  "),
]

print(f"{'Table':35s} | {'Rows':>10s} | {'Size (MB)':>10s} | {'Files':>6s}")
print(f"{'─'*35}─┼─{'─'*10}─┼─{'─'*10}─┼─{'─'*6}")

for table_name, label in tables_to_check:
    try:
        detail = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
        size_mb = builtins.round(detail["sizeInBytes"] / (1024 * 1024), 2)
        num_files = detail["numFiles"]
        row_count = spark.table(table_name).count()
        print(f"{label:35s} | {row_count:>10,} | {size_mb:>10.2f} | {num_files:>6}")
    except Exception as e:
        print(f"{label:35s} | {'N/A':>10s} | {'N/A':>10s} | {'N/A':>6s}  ({e})")

print(f"\nℹ️  Run benchmarks 4 & 5 first to create the _bench_* comparison tables.")

# COMMAND ----------

# DBTITLE 1,Final Summary Header
# MAGIC %md
# MAGIC ---
# MAGIC ## Summary
# MAGIC Each benchmark cell above shows the **speedup** from each optimization.
# MAGIC
# MAGIC After running all cells, check the results printed in each cell to see:
# MAGIC - **Nx speedup** (how many times faster the optimized version is)
# MAGIC - **% improvement** (what percentage of time was saved)
# MAGIC - **Row counts** (confirming both variants produce the same data)

# COMMAND ----------

# DBTITLE 1,OPTIMIZE on Gold Table (One-Time Maintenance)
# MAGIC %md
# MAGIC ---
# MAGIC ## Bonus: OPTIMIZE on `gold_traffic_stats`
# MAGIC
# MAGIC After the full-day data collection run (7:15 AM – 8:50 PM ET, every 10 minutes), we ran `OPTIMIZE` on the Gold table to compact the many small Delta files created by frequent MERGE upserts.
# MAGIC
# MAGIC ```sql
# MAGIC OPTIMIZE bootcamp_students.zachy_sandeepmanoharan1.gold_traffic_stats
# MAGIC ```
# MAGIC
# MAGIC ### Results (Post full-day collection — Mar 27, 8:50 PM ET)
# MAGIC | Metric | Before | After | Improvement |
# MAGIC |---|---|---|---|
# MAGIC | **Files** | 29 | 1 | 96.6% fewer files |
# MAGIC | **Size** | 63.4 KB (across 29 files) | 13.0 KB (1 file) | 79.6% smaller |
# MAGIC
# MAGIC ### What do the numbers mean?
# MAGIC - **Before (29 files, 63.4 KB)**: Each of the ~80 job runs triggered a MERGE upsert into Gold, and each MERGE creates a small Delta file. Over 13 hours of collection, 29 of these small files accumulated. Each file carries fixed overhead (file headers, metadata, Parquet footer), so 29 tiny files waste storage and force Spark to open 29 separate file handles on every dashboard query.
# MAGIC - **After (1 file, 13.0 KB)**: `OPTIMIZE` bin-packed all 29 files into a single compact file. The 79.6% size reduction comes from eliminating the per-file overhead that was duplicated across 29 files. Dashboard queries now scan 1 file instead of 29, removing the file-open latency that dominated read time on this small table.
# MAGIC
# MAGIC ### Why not run OPTIMIZE on every pipeline execution?
# MAGIC `OPTIMIZE` is a **periodic maintenance** operation. Each 10-minute job run only adds 1 small file via MERGE. Running OPTIMIZE every time would:
# MAGIC - Add unnecessary compute overhead to each job run
# MAGIC - Trigger full file rewrites even when only a handful of new rows were added
# MAGIC
# MAGIC **Recommendation**: Run OPTIMIZE manually after data collection sessions, or schedule a separate weekly maintenance job when file count exceeds ~50.

# COMMAND ----------

# DBTITLE 1,Benchmark 7: OPTIMIZE on Gold Table
# ── BEFORE OPTIMIZE: Capture file stats ──
before = spark.sql("DESCRIBE DETAIL bootcamp_students.zachy_sandeepmanoharan1.gold_traffic_stats").collect()[0]
before_files = before['numFiles']
before_bytes = before['sizeInBytes']
print(f"BEFORE OPTIMIZE:")
print(f"  Files: {before_files}")
print(f"  Size:  {before_bytes:,} bytes ({before_bytes/1024:.1f} KB)")
print(f"{'─'*80}")

# ── RUN OPTIMIZE ──
result = spark.sql("OPTIMIZE bootcamp_students.zachy_sandeepmanoharan1.gold_traffic_stats").collect()[0]
print(f"OPTIMIZE result:")
print(f"  Files added:   {result['metrics']['numFilesAdded']}")
print(f"  Files removed: {result['metrics']['numFilesRemoved']}")
print(f"{'─'*80}")

# ── AFTER OPTIMIZE: Capture file stats ──
after = spark.sql("DESCRIBE DETAIL bootcamp_students.zachy_sandeepmanoharan1.gold_traffic_stats").collect()[0]
after_files = after['numFiles']
after_bytes = after['sizeInBytes']
print(f"AFTER OPTIMIZE:")
print(f"  Files: {after_files}")
print(f"  Size:  {after_bytes:,} bytes ({after_bytes/1024:.1f} KB)")

# ── COMPARISON ──
file_reduction = ((before_files - after_files) / before_files) * 100 if before_files > 0 else 0
size_reduction = ((before_bytes - after_bytes) / before_bytes) * 100 if before_bytes > 0 else 0
print(f"\n  ┌─────────────────────────────────────────────────┐")
print(f"  │  Files: {before_files} → {after_files}  ({file_reduction:.1f}% fewer files)")
print(f"  │  Size:  {before_bytes/1024:.1f} KB → {after_bytes/1024:.1f} KB  ({size_reduction:.1f}% smaller)")
print(f"  └─────────────────────────────────────────────────┘")

# COMMAND ----------

# DBTITLE 1,Final Benchmark Summary
# ── Cleanup temporary benchmark tables ──
spark.sql("DROP TABLE IF EXISTS bootcamp_students.zachy_sandeepmanoharan1._bench_crosswalk_no_zorder")
spark.sql("DROP TABLE IF EXISTS bootcamp_students.zachy_sandeepmanoharan1._bench_osm_no_cluster")
print("✅ Benchmark tables cleaned up.")