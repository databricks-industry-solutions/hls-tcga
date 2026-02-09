# Databricks notebook source
"""
Configuration Loader for TCGA Pipeline

This notebook loads configuration from config.json and stores it in a Delta table.
Each configuration gets a unique ID and timestamp for full traceability.

This should be the first task in the workflow, and all subsequent tasks read
configuration from the Delta table using the config_id.
"""

# COMMAND ----------

import json
import uuid
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Configuration from config.json

# COMMAND ----------

# Read config.json
try:
    with open('./config.json', 'r') as f:
        config = json.load(f)
    print("✓ Loaded configuration from config.json")
except FileNotFoundError:
    raise FileNotFoundError(
        "config.json not found. Please run deploy.py first to create configuration."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare Configuration Record

# COMMAND ----------

# Generate unique config ID and timestamp
config_id = str(uuid.uuid4())
config_timestamp = datetime.now()

# Flatten configuration into a single record
config_record = {
    'config_id': config_id,
    'config_timestamp': config_timestamp,
    'catalog': config['lakehouse']['catalog'],
    'schema': config['lakehouse']['schema'],
    'volume': config['lakehouse']['volume'],
    'volume_path': f"/Volumes/{config['lakehouse']['catalog']}/{config['lakehouse']['schema']}/{config['lakehouse']['volume']}",
    'database_name': f"{config['lakehouse']['catalog']}.{config['lakehouse']['schema']}",
    'max_workers': config['pipeline']['max_workers'],
    'max_records': config['pipeline']['max_records'],
    'force_download': config['pipeline']['force_download'],
    'retry_attempts': config['pipeline']['retry_attempts'],
    'timeout_seconds': config['pipeline']['timeout_seconds'],
    'cases_endpt': config['api_paths']['cases_endpt'],
    'files_endpt': config['api_paths']['files_endpt'],
    'data_endpt': config['api_paths']['data_endpt'],
    'download_node_type': config['compute']['download_node_type'],
    'etl_node_type': config['compute']['etl_node_type'],
    'analysis_node_type': config['compute']['analysis_node_type'],
    'deployment_profile': config['deployment']['profile'],
    'deployment_cloud': config['deployment']['cloud'],
    'is_active': True  # Mark this as the active configuration
}

print(f"✓ Generated configuration ID: {config_id}")
print(f"✓ Timestamp: {config_timestamp}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create/Update Configuration Table

# COMMAND ----------

# Define schema for configuration table
config_schema = StructType([
    StructField("config_id", StringType(), False),
    StructField("config_timestamp", TimestampType(), False),
    StructField("catalog", StringType(), False),
    StructField("schema", StringType(), False),
    StructField("volume", StringType(), False),
    StructField("volume_path", StringType(), False),
    StructField("database_name", StringType(), False),
    StructField("max_workers", IntegerType(), False),
    StructField("max_records", IntegerType(), False),
    StructField("force_download", BooleanType(), False),
    StructField("retry_attempts", IntegerType(), False),
    StructField("timeout_seconds", IntegerType(), False),
    StructField("cases_endpt", StringType(), False),
    StructField("files_endpt", StringType(), False),
    StructField("data_endpt", StringType(), False),
    StructField("download_node_type", StringType(), True),
    StructField("etl_node_type", StringType(), True),
    StructField("analysis_node_type", StringType(), True),
    StructField("deployment_profile", StringType(), True),
    StructField("deployment_cloud", StringType(), True),
    StructField("is_active", BooleanType(), False)
])

# Configuration table name
config_table = f"{config_record['database_name']}.pipeline_config"

# COMMAND ----------

# Create DataFrame from config record
config_df = spark.createDataFrame([config_record], schema=config_schema)

# COMMAND ----------

# Check if table exists
table_exists = spark.catalog.tableExists(config_table)

if table_exists:
    print(f"✓ Configuration table '{config_table}' exists")

    # Mark all previous configs as inactive
    spark.sql(f"""
        UPDATE {config_table}
        SET is_active = false
        WHERE is_active = true
    """)
    print("✓ Marked previous configurations as inactive")

    # Append new configuration
    config_df.write.mode("append").saveAsTable(config_table)
    print(f"✓ Added new configuration with ID: {config_id}")
else:
    print(f"Creating configuration table '{config_table}'...")

    # Create table with new configuration
    config_df.write.mode("overwrite").saveAsTable(config_table)

    print(f"✓ Created configuration table '{config_table}'")
    print(f"✓ Added configuration with ID: {config_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Configuration

# COMMAND ----------

# Read back the active configuration
active_config = spark.sql(f"""
    SELECT *
    FROM {config_table}
    WHERE is_active = true
    ORDER BY config_timestamp DESC
    LIMIT 1
""")

print("\n" + "="*70)
print("Active Configuration:")
print("="*70)
active_config.show(truncate=False, vertical=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export Configuration ID for Downstream Tasks

# COMMAND ----------

# Create widget with config_id for downstream tasks
dbutils.widgets.text("config_id", config_id, "Configuration ID")

# Export as notebook output for job orchestration
dbutils.notebook.exit(json.dumps({
    "config_id": config_id,
    "config_timestamp": config_timestamp.isoformat(),
    "status": "success",
    "message": f"Configuration loaded successfully with ID: {config_id}"
}))
