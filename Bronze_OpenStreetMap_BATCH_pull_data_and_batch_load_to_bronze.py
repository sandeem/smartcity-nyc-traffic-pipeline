# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze OSM Roads — One-Time Batch Load
# MAGIC
# MAGIC **Pipeline Stage**: Geofabrik (OpenStreetMap) → `bronze_osm_roads` (Delta)
# MAGIC
# MAGIC **What this notebook does**: Downloads the full New York State road network from OpenStreetMap (via Geofabrik), converts the shapefile geometries to WKT text, and loads 1.8 million road segments into a managed Unity Catalog Delta table. This is a **one-time setup** — the data is static reference data that the streaming pipeline (Part C) joins against.
# MAGIC
# MAGIC | Detail | Value |
# MAGIC |---|---|
# MAGIC | **Source** | [Geofabrik NY Shapefile](https://download.geofabrik.de/north-america/us/new-york-latest-free.shp.zip) |
# MAGIC | **Target** | `bootcamp_students.zachy_sandeepmanoharan1.bronze_osm_roads` |
# MAGIC | **Rows** | 1,825,656 road segments |
# MAGIC | **Method** | Batch load (`wget` → `geopandas` → intermediate Parquet → Delta `saveAsTable`) |
# MAGIC | **Frequency** | One-time (roads don't change frequently) |
# MAGIC | **Dependencies** | `geopandas` (installed via `%pip install`) |

# COMMAND ----------

# DBTITLE 1,Download Section
# MAGIC %md
# MAGIC ---
# MAGIC ### Step 1: Download OSM Shapefile from Geofabrik
# MAGIC
# MAGIC **Logic**: Downloads the New York State road network shapefile from Geofabrik (an OpenStreetMap data mirror), saves it to a Unity Catalog Volume, and unzips the contents.
# MAGIC
# MAGIC **Key terms:**
# MAGIC - **OpenStreetMap (OSM)**: A free, open-source map of the world — like Wikipedia but for maps. Contributors worldwide add roads, buildings, and geographic features.
# MAGIC - **Geofabrik**: A third-party mirror that provides OSM data as downloadable shapefiles, organized by region (country, state).
# MAGIC - **Shapefile (.shp)**: A geospatial file format that stores road geometries (lines, polygons) along with attribute data (road name, type, speed limit). Requires multiple companion files (.shx, .dbf, .prj).
# MAGIC - **Unity Catalog Volume**: A managed storage location governed by UC. Used here as a staging area for the raw download before loading into a Delta table.
# MAGIC - **`subprocess.run`**: Runs a shell command (`wget`) from Python. `check=True` ensures the script stops if the download fails.
# MAGIC - **`zipfile.ZipFile`**: Python’s built-in library for extracting zip archives. The `with` statement ensures the file handle is properly closed.

# COMMAND ----------

# DBTITLE 1,Download and Unzip OSM Shapefile
# 1. Create a staging folder inside the Unity Catalog Volume
volume_path = "/Volumes/bootcamp_students/zachy_sandeepmanoharan1/capstone/batch_osm"
dbutils.fs.mkdirs(volume_path)

# 2. Download OSM shapefile from Geofabrik directly into the Volume
# wget -q: Quiet mode (no progress bar) | -O: Output to specific filename
import subprocess
subprocess.run(
    ["wget", "-q", "-O", f"{volume_path}/osm.zip",
     "https://download.geofabrik.de/north-america/us/new-york-latest-free.shp.zip"],
    check=True  # Stops and throws an error if the download fails
)

# 3. Unzip the shapefile contents into a sub-folder
# 'with' ensures the file handle is properly closed after extraction
import zipfile
with zipfile.ZipFile(f"{volume_path}/osm.zip", "r") as z:
    z.extractall(f"{volume_path}/unzipped")

print(f"Download complete. Files extracted to {volume_path}/unzipped/")

# COMMAND ----------

# DBTITLE 1,Verify Extracted Files
# MAGIC %fs ls /Volumes/bootcamp_students/zachy_sandeepmanoharan1/capstone/batch_osm/unzipped/

# COMMAND ----------

# DBTITLE 1,Conversion Section
# MAGIC %md
# MAGIC ---
# MAGIC ### Step 2: Convert Shapefile to Delta Table
# MAGIC
# MAGIC **Logic**: Reads the road shapefile with `geopandas`, converts geometry objects to WKT (Well-Known Text) strings, writes to an intermediate Parquet file, then loads into a managed Unity Catalog Delta table.
# MAGIC
# MAGIC **Key terms:**
# MAGIC - **geopandas**: A Python library that extends pandas to handle geospatial data. Reads shapefiles directly into DataFrames with geometry columns.
# MAGIC - **WKT (Well-Known Text)**: A text format for representing geometries. Example: `LINESTRING (-79.03 43.15, -79.02 43.14)`. Used because Spark DataFrames don’t support native geometry objects in the Bronze layer.
# MAGIC - **Intermediate Parquet workaround**: `spark.createDataFrame(pandas_df)` serializes the entire DataFrame into a single RPC message. With 1.8M rows of road geometry (large WKT strings), this exceeded the 268MB `spark.rpc.message.maxSize` limit. Writing to Parquet first and reading with Spark bypasses this bottleneck.
# MAGIC - **`saveAsTable()`**: Writes the DataFrame as a managed Unity Catalog Delta table. UC owns the underlying files and handles storage, versioning, and cleanup.

# COMMAND ----------

# DBTITLE 1,Install geopandas Library
# MAGIC %pip install geopandas --quiet

# COMMAND ----------

# DBTITLE 1,Restart Python Kernel (Required After pip Install)
# MAGIC %restart_python

# COMMAND ----------

# DBTITLE 1,Convert Shapefile to Bronze Delta Table
import geopandas as gpd
import pandas as pd

# 1. Read the New York State road shapefile with geopandas
# This takes ~1 minute due to 1.8M rows of road geometry
shapefile_path = "/Volumes/bootcamp_students/zachy_sandeepmanoharan1/capstone/batch_osm/unzipped/gis_osm_roads_free_1.shp"
gdf = gpd.read_file(shapefile_path)

# 2. Convert geometry objects to WKT text strings
# Spark DataFrames don't handle native geometry objects in Bronze layer
# FIX: Write to intermediate Parquet to avoid 268MB spark.rpc.message.maxSize limit
# (spark.createDataFrame(pandas_df) serializes entire DF into a single RPC message)
tmp_parquet = "/Volumes/bootcamp_students/zachy_sandeepmanoharan1/capstone/batch_osm/tmp_roads.parquet"
pdf = pd.DataFrame(gdf).drop(columns='geometry').assign(geometry_wkt=gdf.geometry.to_wkt())
pdf.to_parquet(tmp_parquet)
df_osm = spark.read.parquet(tmp_parquet)

# 3. Write to managed Unity Catalog Delta table (one-time overwrite)
table_name = "bootcamp_students.zachy_sandeepmanoharan1.bronze_osm_roads"
df_osm.write.format("delta").mode("overwrite").saveAsTable(table_name)

print(f"Success! {df_osm.count():,} road segments saved to {table_name}")

# COMMAND ----------

# DBTITLE 1,Verification Section
# MAGIC %md
# MAGIC ---
# MAGIC ### Verification: Query Bronze OSM Data
# MAGIC Optional cells to inspect the loaded OSM data and verify the row count (should be ~1.8M). These confirm the one-time batch load completed successfully.
# MAGIC
# MAGIC **Key terms:**
# MAGIC - **Bronze Layer**: The first layer of the Medallion Architecture. Contains raw, unprocessed data exactly as it arrived from the source — no transformations, no filtering. The OSM data here is static reference data, unlike `bronze_traffic` which grows continuously.

# COMMAND ----------

# DBTITLE 1,Verify OSM Road Count
# MAGIC %sql
# MAGIC -- Verify total row count (should be ~1,825,656)
# MAGIC SELECT count(*) AS total_osm_roads FROM bootcamp_students.zachy_sandeepmanoharan1.bronze_osm_roads;

# COMMAND ----------

# DBTITLE 1,Preview Bronze OSM Roads
# MAGIC %sql
# MAGIC -- Preview sample road segments with names
# MAGIC SELECT osm_id, name, fclass, ref, maxspeed,
# MAGIC        SUBSTRING(geometry_wkt, 1, 80) AS geometry_preview
# MAGIC FROM bootcamp_students.zachy_sandeepmanoharan1.bronze_osm_roads
# MAGIC WHERE name IS NOT NULL
# MAGIC LIMIT 20;