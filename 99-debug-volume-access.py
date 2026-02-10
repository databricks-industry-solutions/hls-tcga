# Databricks notebook source
"""
Volume Access Diagnostic Tool

This notebook helps diagnose volume access issues by checking:
1. Volume existence and permissions
2. File contents in the volume
3. User identity and permissions
4. Common permission issues
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Current User and Permissions

# COMMAND ----------

# Get current user identity
current_user = spark.sql("SELECT current_user() as user").collect()[0]['user']
print(f"Running as user: {current_user}")

# Check if we're running under a service principal
try:
    user_info = spark.sql("SELECT current_user(), session_user()").collect()[0]
    print(f"Current user: {user_info[0]}")
    print(f"Session user: {user_info[1]}")
except:
    print("Could not get detailed user info")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Configuration and Check Volume

# COMMAND ----------

from utils.config_loader import ConfigLoader

# Create widgets
dbutils.widgets.text("catalog", "kermany", "Catalog")
dbutils.widgets.text("schema", "tcga", "Schema")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

print(f"Checking catalog: {catalog}, schema: {schema}")

# COMMAND ----------

# Load configuration
try:
    loader = ConfigLoader(spark, catalog, schema)
    config = loader.get_active_config()

    volume_path = config['volume_path']
    volume_name = config['volume']

    print(f"✓ Configuration loaded")
    print(f"  Volume name: {volume_name}")
    print(f"  Volume path: {volume_path}")
except Exception as e:
    print(f"✗ Could not load configuration: {e}")
    # Fall back to direct values
    volume_name = "tcga_files"
    volume_path = f"/Volumes/{catalog}/{schema}/{volume_name}"
    print(f"Using fallback: {volume_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Volume Exists

# COMMAND ----------

# Check if volume exists in Unity Catalog
try:
    volumes_df = spark.sql(f"SHOW VOLUMES IN {catalog}.{schema}")
    volumes_list = [row['volume_name'] for row in volumes_df.collect()]

    print(f"Volumes in {catalog}.{schema}:")
    for vol in volumes_list:
        print(f"  - {vol}")

    if volume_name in volumes_list:
        print(f"\n✓ Volume '{volume_name}' exists")
    else:
        print(f"\n✗ Volume '{volume_name}' NOT FOUND")
        print(f"\nTo create it, run:")
        print(f"  spark.sql('CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume_name}')")
except Exception as e:
    print(f"✗ Error checking volumes: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Volume Permissions

# COMMAND ----------

# Try to describe the volume
try:
    volume_info = spark.sql(f"DESCRIBE VOLUME {catalog}.{schema}.{volume_name}").collect()
    print(f"✓ Can describe volume {catalog}.{schema}.{volume_name}")
    print("\nVolume details:")
    for row in volume_info:
        print(f"  {row['info_name']}: {row['info_value']}")
except Exception as e:
    print(f"✗ Cannot describe volume: {e}")
    print(f"\nPossible issues:")
    print(f"  1. Volume doesn't exist")
    print(f"  2. No USE CATALOG permission on '{catalog}'")
    print(f"  3. No USE SCHEMA permission on '{schema}'")
    print(f"  4. No READ VOLUME permission on '{volume_name}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Try to List Files in Volume

# COMMAND ----------

# Try to list files using dbutils
try:
    print(f"Listing files in: {volume_path}")
    files = dbutils.fs.ls(volume_path)

    print(f"\n✓ Successfully listed {len(files)} items:")
    for file in files[:10]:  # Show first 10
        size_mb = file.size / (1024 * 1024)
        print(f"  - {file.name} ({size_mb:.2f} MB)")

    if len(files) > 10:
        print(f"  ... and {len(files) - 10} more items")

    if len(files) == 0:
        print(f"\n⚠ Volume is EMPTY!")
        print(f"Run the download task (01-data-download.py) to populate it.")

except Exception as e:
    print(f"✗ Cannot list files: {e}")
    print(f"\nThis usually means:")
    print(f"  1. Volume path is incorrect")
    print(f"  2. No READ VOLUME permission")
    print(f"  3. Volume doesn't exist")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Expected Files for DLT Pipeline

# COMMAND ----------

expected_files = [
    "expressions_info.tsv",
    "cases.tsv",
    "expressions/"
]

print("Checking for expected files:")
for expected in expected_files:
    file_path = f"{volume_path}/{expected}"
    try:
        files = dbutils.fs.ls(file_path)
        print(f"  ✓ {expected} exists")
        if expected.endswith('/'):
            print(f"    Contains {len(files)} files")
    except Exception as e:
        print(f"  ✗ {expected} NOT FOUND")
        print(f"    Error: {str(e)[:100]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Try to Read a Sample File

# COMMAND ----------

# Try to read expressions_info.tsv
try:
    sample_file = f"{volume_path}/expressions_info.tsv"
    print(f"Attempting to read: {sample_file}")

    df = spark.read.csv(sample_file, sep='\t', header=True, inferSchema=True)
    count = df.count()

    print(f"✓ Successfully read file!")
    print(f"  Rows: {count}")
    print(f"  Columns: {len(df.columns)}")
    print(f"\nFirst few rows:")
    df.show(5, truncate=False)

except Exception as e:
    print(f"✗ Cannot read file: {e}")
    print(f"\nThis is the same error the DLT pipeline is experiencing!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Grants on Volume

# COMMAND ----------

# Try to show grants
try:
    grants = spark.sql(f"SHOW GRANTS ON VOLUME {catalog}.{schema}.{volume_name}").collect()
    print(f"Grants on {catalog}.{schema}.{volume_name}:")
    for grant in grants:
        print(f"  - {grant['principal']}: {grant['action_type']}")

    # Check if current user has permissions
    current_user_grants = [g for g in grants if current_user in str(g['principal'])]
    if current_user_grants:
        print(f"\n✓ Current user ({current_user}) has grants")
    else:
        print(f"\n⚠ Current user ({current_user}) has NO grants!")
        print(f"\nTo fix, run:")
        print(f"  GRANT READ VOLUME ON VOLUME {catalog}.{schema}.{volume_name} TO `{current_user}`;")
        print(f"  GRANT WRITE VOLUME ON VOLUME {catalog}.{schema}.{volume_name} TO `{current_user}`;")

except Exception as e:
    print(f"Could not check grants: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recommendations

# COMMAND ----------

print("=" * 70)
print("DIAGNOSTIC SUMMARY")
print("=" * 70)

recommendations = []

# Check 1: Volume exists
try:
    spark.sql(f"DESCRIBE VOLUME {catalog}.{schema}.{volume_name}")
    print("✓ Volume exists")
except:
    print("✗ Volume does NOT exist")
    recommendations.append(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume_name}")

# Check 2: Can list files
try:
    files = dbutils.fs.ls(volume_path)
    if len(files) > 0:
        print(f"✓ Volume has {len(files)} items")
    else:
        print("⚠ Volume is EMPTY")
        recommendations.append("Run 01-data-download.py to populate the volume")
except:
    print("✗ Cannot access volume")
    recommendations.append(f"GRANT READ VOLUME ON VOLUME {catalog}.{schema}.{volume_name} TO `{current_user}`")
    recommendations.append(f"GRANT WRITE VOLUME ON VOLUME {catalog}.{schema}.{volume_name} TO `{current_user}`")

# Check 3: Can read files
try:
    df = spark.read.csv(f"{volume_path}/expressions_info.tsv", sep='\t', header=True)
    df.count()
    print("✓ Can read files from volume")
except:
    print("✗ Cannot read files")

if recommendations:
    print("\n" + "=" * 70)
    print("RECOMMENDATIONS:")
    print("=" * 70)
    for i, rec in enumerate(recommendations, 1):
        print(f"{i}. {rec}")
else:
    print("\n✓ All checks passed! Volume should be accessible.")
    print("\nIf DLT pipeline still fails, check:")
    print("  1. DLT pipeline is using run_as with correct username")
    print("  2. Service principal permissions (if applicable)")
    print("  3. Check DLT pipeline logs for detailed error messages")
