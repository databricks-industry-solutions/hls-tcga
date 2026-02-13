# Configuration Management Guide

## Overview

The TCGA pipeline uses a **Delta table-based configuration system** that provides:

- **Versioning**: Every configuration gets a unique ID and timestamp
- **Audit Trail**: Track which configuration was used for each pipeline run
- **Consistency**: All tasks in a run use the same configuration snapshot
- **Traceability**: Link results back to the exact configuration used

## Configuration Flow

```
config.json → Delta Table → All Pipeline Tasks
     ↓            ↓              ↓
  deploy.py   load_config   read from table
```

1. **Create/Update**: User creates `config.json` via `deploy.py` or manually
2. **Load**: Job workflow starts with `00-load-config.py` which loads config into Delta table
3. **Use**: All subsequent tasks read configuration from the Delta table

## Configuration Table Schema

The configuration table `{catalog}.{schema}.pipeline_config` contains:

| Column | Type | Description |
|--------|------|-------------|
| `config_id` | STRING | Unique UUID for this configuration |
| `config_timestamp` | TIMESTAMP | When this config was loaded |
| `is_active` | BOOLEAN | Whether this is the currently active config |
| `catalog` | STRING | Unity Catalog name |
| `schema` | STRING | Schema name |
| `volume` | STRING | Volume name |
| `volume_path` | STRING | Full volume path |
| `database_name` | STRING | Full database name (catalog.schema) |
| `max_workers` | INT | Max concurrent download workers |
| `max_records` | INT | Max records to download |
| `force_download` | BOOLEAN | Whether to force re-download |
| `retry_attempts` | INT | Number of retry attempts |
| `timeout_seconds` | INT | Operation timeout |
| `cases_endpt` | STRING | GDC cases API endpoint |
| `files_endpt` | STRING | GDC files API endpoint |
| `data_endpt` | STRING | GDC data API endpoint |
| `download_node_type` | STRING | Download cluster node type |
| `etl_node_type` | STRING | ETL cluster node type |
| `analysis_node_type` | STRING | Analysis cluster node type |
| `deployment_profile` | STRING | Databricks profile used |
| `deployment_cloud` | STRING | Cloud provider (aws/azure) |

## Using Configuration in Notebooks

### Method 1: Using ConfigLoader Utility (Recommended)

```python
# At the start of your notebook
from utils.config_loader import ConfigLoader

# Create widgets for catalog and schema (passed from job)
dbutils.widgets.text("catalog", "kermany", "Catalog")
dbutils.widgets.text("schema", "tcga", "Schema")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# Load configuration
loader = ConfigLoader(spark, catalog, schema)
config = loader.get_active_config()

# Access configuration values
volume_path = config['volume_path']
max_workers = config['max_workers']
api_endpoints = loader.get_api_config()
```

### Method 2: Direct SQL Query

```python
# Simple direct query
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

config_df = spark.sql(f"""
    SELECT *
    FROM {catalog}.{schema}.pipeline_config
    WHERE is_active = true
    ORDER BY config_timestamp DESC
    LIMIT 1
""")

config = config_df.first().asDict()
volume_path = config['volume_path']
```

### Method 3: Using Convenience Functions

```python
from utils.config_loader import get_active_config

# Get full config
config = get_active_config(spark, "kermany", "tcga")

# Access specific sections
from utils.config_loader import ConfigLoader
loader = ConfigLoader(spark, "kermany", "tcga")

lakehouse_config = loader.get_lakehouse_config()
api_config = loader.get_api_config()
pipeline_config = loader.get_pipeline_config()
```

## Updating Existing Notebooks

### Before (Old Approach):

```python
# Old way: Reading from config.json directly
import json

with open('./config.json', 'r') as f:
    config = json.load(f)

catalog = config['lakehouse']['catalog']
schema = config['lakehouse']['schema']
volume = config['lakehouse']['volume']
volume_path = f"/Volumes/{catalog}/{schema}/{volume}"
max_workers = config['pipeline']['max_workers']
```

### After (New Approach):

```python
# New way: Reading from Delta table
from utils.config_loader import ConfigLoader

# Get catalog/schema from job parameters
dbutils.widgets.text("catalog", "kermany", "Catalog")
dbutils.widgets.text("schema", "tcga", "Schema")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# Load configuration from Delta table
loader = ConfigLoader(spark, catalog, schema)
config = loader.get_active_config()

# Access values
volume_path = config['volume_path']
max_workers = config['max_workers']
```

## Example: Complete Notebook Pattern

```python
# Databricks notebook source
"""
My Pipeline Task

This notebook demonstrates the standard pattern for accessing configuration.
"""

# COMMAND ----------

# Import configuration utilities
from utils.config_loader import ConfigLoader

# COMMAND ----------

# Create widgets (job will pass these values)
dbutils.widgets.text("catalog", "", "Catalog Name")
dbutils.widgets.text("schema", "", "Schema Name")

# Get values
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

if not catalog or not schema:
    raise ValueError("catalog and schema must be provided")

# COMMAND ----------

# Load configuration
print(f"Loading configuration from {catalog}.{schema}.pipeline_config...")
loader = ConfigLoader(spark, catalog, schema)
config = loader.get_active_config()

print(f"✓ Loaded configuration ID: {config['config_id']}")
print(f"✓ Timestamp: {config['config_timestamp']}")

# COMMAND ----------

# Access configuration values
volume_path = config['volume_path']
database_name = config['database_name']
max_workers = config['max_workers']
api_endpoints = loader.get_api_config()

print(f"Volume path: {volume_path}")
print(f"Database: {database_name}")
print(f"Max workers: {max_workers}")

# COMMAND ----------

# Your notebook logic here
# Use config values as needed
spark.sql(f"USE CATALOG {config['catalog']}")
spark.sql(f"USE SCHEMA {config['schema']}")

# ... rest of your code ...
```

## Job Workflow Integration

In `resources/jobs.yml`, the workflow now includes config loading as the first task:

```yaml
tasks:
  # Task 1: Load configuration
  - task_key: "load_config"
    notebook_task:
      notebook_path: ../00-load-config.py

  # Task 2: Downstream tasks depend on load_config
  - task_key: "my_task"
    depends_on:
      - task_key: "load_config"
    notebook_task:
      notebook_path: ../my-task.py
      base_parameters:
        catalog: "${var.catalog_name}"
        schema: "${var.schema_name}"
```

## Viewing Configuration History

### List Recent Configurations

```python
from utils.config_loader import ConfigLoader

loader = ConfigLoader(spark, "kermany", "tcga")
loader.list_configs(limit=10)
```

### Query Configuration Table Directly

```sql
SELECT
    config_id,
    config_timestamp,
    is_active,
    max_workers,
    max_records,
    deployment_cloud
FROM kermany.tcga.pipeline_config
ORDER BY config_timestamp DESC
LIMIT 10
```

### Get Configuration Used by Specific Run

```python
# If you stored config_id with your results
config_id = "abc-123-def-456"
loader = ConfigLoader(spark, "kermany", "tcga")
config = loader.get_config_by_id(config_id)
```

## Best Practices

1. **Always Load Config First**: Make `load_config` the first task in your workflow

2. **Pass Catalog/Schema**: All tasks should receive catalog and schema as parameters:
   ```yaml
   base_parameters:
     catalog: "${var.catalog_name}"
     schema: "${var.schema_name}"
   ```

3. **Use ConfigLoader Utility**: Prefer using `ConfigLoader` class over direct SQL queries

4. **Store Config ID with Results**: When writing analysis results, include the config_id:
   ```python
   results_df = results_df.withColumn("config_id", lit(config['config_id']))
   ```

5. **Mark Active Config**: Only one configuration should have `is_active=true` at a time

6. **Never Modify Config Table Manually**: Always use `00-load-config.py` to update configs

## Troubleshooting

### Error: "No active configuration found"

**Solution**: Run the `00-load-config.py` notebook first, or ensure `load_config` task completed successfully.

### Error: "Table pipeline_config does not exist"

**Solution**: The config table is created automatically on first run. Ensure:
1. Schema exists in Unity Catalog
2. You have write permissions to the schema
3. Run `00-load-config.py` to initialize the table

### Multiple Active Configurations

**Solution**: Manually fix by running:
```sql
UPDATE {catalog}.{schema}.pipeline_config
SET is_active = false
WHERE config_id != '<desired-config-id>';

UPDATE {catalog}.{schema}.pipeline_config
SET is_active = true
WHERE config_id = '<desired-config-id>';
```

## Migration Checklist

To migrate existing notebooks to use Delta table configuration:

- [ ] Add `from utils.config_loader import ConfigLoader` import
- [ ] Add catalog/schema widgets
- [ ] Replace config.json reading with `ConfigLoader.get_active_config()`
- [ ] Update all config access to use dictionary keys (e.g., `config['volume_path']`)
- [ ] Test notebook with active configuration in Delta table
- [ ] Update job YAML to pass catalog/schema parameters
- [ ] Ensure `load_config` task runs before this task

## Next Steps

See these notebooks for complete examples:
- `00-load-config.py` - Configuration loading implementation
- `utils/config_loader.py` - Configuration utility module
- Individual pipeline notebooks for usage patterns
