# Configuration Migration Summary

## What Changed

The TCGA pipeline has been migrated from file-based configuration (config.json) to a **Delta table-based configuration system** for better traceability, versioning, and auditability.

## Files Updated

### 1. **00-load-config.py** (NEW)
- Loads config.json into Delta table
- Creates unique ID and timestamp for each configuration
- Marks previous configs as inactive
- Exports config_id for downstream tasks

### 2. **utils/config_loader.py** (NEW)
- ConfigLoader class for accessing configuration from Delta table
- Convenience methods for getting specific config sections
- Caching support for performance

### 3. **01-data-download.py** (UPDATED)
**Before:**
```python
# MAGIC %run ./00-setup

# Old way: Variables came from 00-setup notebook
volume_path = ...
cases_endpt = ...
```

**After:**
```python
from utils.config_loader import ConfigLoader

# Create widgets
dbutils.widgets.text("catalog", "kermany", "Catalog")
dbutils.widgets.text("schema", "tcga", "Schema")

# Load config from Delta table
config_loader = ConfigLoader(spark, catalog, schema)
config = config_loader.get_active_config()

# Access values
volume_path = config['volume_path']
cases_endpt = config['cases_endpt']
```

### 4. **02-tcga-expression-clustering-optimized.py** (UPDATED)
**Before:**
```python
from config.config_manager import load_config
config = load_config(environment=environment)
catalog = config.lakehouse.catalog
```

**After:**
```python
from utils.config_loader import ConfigLoader

# Get catalog/schema from job parameters
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# Load from Delta table
config_loader = ConfigLoader(spark, catalog, schema)
config = config_loader.get_active_config()

# Access values
config_id = config['config_id']
volume_path = config['volume_path']
```

**Results now include config_id:**
```python
# Add config_id to results for traceability
results_df = results_df.withColumn("config_id", lit(config_id))
results_df = results_df.withColumn("analysis_timestamp", current_timestamp())
```

### 5. **etl_pipelines/transformations/data_ingestion.py** (UPDATED)
**Before:**
```python
import json
config_path = os.path.abspath('../../config.json')
with open(config_path, 'r') as file:
    config = json.load(file)
CATALOG = config['lakehouse']['catalog']
```

**After:**
```python
# Load from DLT pipeline configuration (set in pipelines.yml)
CATALOG = spark.conf.get("catalog")
SCHEMA = spark.conf.get("schema")
VOLUME = spark.conf.get("volume")
VOLUME_PATH = f'/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}'
```

### 6. **resources/jobs.yml** (UPDATED)
Added `load_config` task as first step:
```yaml
tasks:
  # Task 1: Load configuration into Delta table
  - task_key: "load_config"
    notebook_task:
      notebook_path: ../00-load-config.py

  # Task 2: Download data (depends on config)
  - task_key: "download_data"
    depends_on:
      - task_key: "load_config"
    notebook_task:
      notebook_path: ../01-data-download.py
      base_parameters:
        catalog: "${var.catalog_name}"
        schema: "${var.schema_name}"
```

## Configuration Flow

```
┌──────────────┐
│  deploy.py   │  Creates config.json locally
└──────┬───────┘
       │
       v
┌──────────────────┐
│ 00-load-config   │  Loads → Delta table (pipeline_config)
│                  │  - Generates UUID
│                  │  - Adds timestamp
│                  │  - Marks as active
└──────┬───────────┘
       │
       ├───────────────────┬────────────────┬──────────────┐
       │                   │                │              │
       v                   v                v              v
┌─────────────┐   ┌─────────────┐   ┌──────────┐   ┌──────────┐
│  Download   │   │     DLT     │   │ Analysis │   │  Future  │
│    Task     │   │  Pipeline   │   │   Task   │   │  Tasks   │
└─────────────┘   └─────────────┘   └──────────┘   └──────────┘
       │                   │                │              │
       └───────────────────┴────────────────┴──────────────┘
                           │
                All read from Delta table
```

## Configuration Table Schema

Table: `{catalog}.{schema}.pipeline_config`

| Column | Type | Description |
|--------|------|-------------|
| config_id | STRING | Unique UUID |
| config_timestamp | TIMESTAMP | When loaded |
| is_active | BOOLEAN | Currently active |
| catalog | STRING | UC catalog name |
| schema | STRING | UC schema name |
| volume | STRING | UC volume name |
| volume_path | STRING | Full volume path |
| database_name | STRING | catalog.schema |
| max_workers | INT | Parallel workers |
| max_records | INT | Download limit |
| force_download | BOOLEAN | Re-download flag |
| retry_attempts | INT | Retry count |
| timeout_seconds | INT | Timeout value |
| cases_endpt | STRING | GDC API endpoint |
| files_endpt | STRING | GDC API endpoint |
| data_endpt | STRING | GDC API endpoint |
| download_node_type | STRING | Cluster type |
| etl_node_type | STRING | Cluster type |
| analysis_node_type | STRING | Cluster type |
| deployment_profile | STRING | Databricks profile |
| deployment_cloud | STRING | aws/azure |

## Benefits

### 1. **Versioning & Audit Trail**
Every configuration has a unique ID and timestamp:
```sql
SELECT
    config_id,
    config_timestamp,
    is_active,
    max_workers,
    max_records
FROM kermany.tcga.pipeline_config
ORDER BY config_timestamp DESC;
```

### 2. **Traceability**
Results tables include config_id:
```sql
-- Find which config produced specific results
SELECT r.*, c.max_workers, c.max_records
FROM kermany.tcga.amir_kermany_expression_clustering_final r
JOIN kermany.tcga.pipeline_config c
  ON r.config_id = c.config_id;
```

### 3. **Reproducibility**
Re-run with exact same configuration:
```python
# Get config from specific run
loader = ConfigLoader(spark, "kermany", "tcga")
old_config = loader.get_config_by_id("abc-123-def-456")

# Reproduce results with same parameters
```

### 4. **Consistency**
All tasks in a workflow use the same config snapshot.

## Testing

### 1. Test Configuration Loading

```bash
# Deploy and run
python deploy.py --run
```

### 2. Verify Configuration Table

In a Databricks notebook:
```python
from utils.config_loader import ConfigLoader

# Load configuration
loader = ConfigLoader(spark, "kermany", "tcga")
config = loader.get_active_config()

# Display
print(f"Config ID: {config['config_id']}")
print(f"Timestamp: {config['config_timestamp']}")
print(f"Volume Path: {config['volume_path']}")

# List history
loader.list_configs(limit=5)
```

### 3. Check Results Include Config ID

```sql
-- Verify results have config_id
SELECT config_id, analysis_timestamp, COUNT(*)
FROM kermany.tcga.amir_kermany_expression_clustering_final
GROUP BY config_id, analysis_timestamp;
```

## Rollback (If Needed)

If issues arise, you can temporarily rollback:

1. **Activate old config:**
```sql
UPDATE kermany.tcga.pipeline_config
SET is_active = false;

UPDATE kermany.tcga.pipeline_config
SET is_active = true
WHERE config_id = '<old-config-id>';
```

2. **Or use old notebooks from git history**

## Documentation

- **CONFIGURATION_GUIDE.md**: Complete usage guide
- **utils/config_loader.py**: API documentation
- **00-load-config.py**: Configuration loading notebook

## Next Steps

1. ✅ Test complete workflow: `python deploy.py --run`
2. ✅ Verify config table created
3. ✅ Check all tasks complete successfully
4. ✅ Verify results include config_id
5. ✅ Query configuration history

## Support

If issues arise:
1. Check config table exists: `SHOW TABLES IN kermany.tcga LIKE 'pipeline_config'`
2. Verify active config: `SELECT * FROM kermany.tcga.pipeline_config WHERE is_active = true`
3. Check logs in job run output
4. See CONFIGURATION_GUIDE.md for troubleshooting
