# TCGA Utilities

This directory contains utility modules for configuration management.

## Modules

### `config_loader.py`

Configuration loading utilities for accessing job configuration stored in Delta tables.

#### Overview

The TCGA pipeline stores configuration in a Delta table (`config_table`) for consistent access across all workflow tasks. The `ConfigLoader` class provides convenient methods to read this configuration.

#### Usage

```python
from config_loader import ConfigLoader

# Initialize loader
config = ConfigLoader(
    catalog="my_catalog",
    schema="my_schema"
)

# Get active configuration
active_config = config.get_active_config()

# Access configuration values
catalog_name = active_config['lakehouse']['catalog']
schema_name = active_config['lakehouse']['schema']
volume_name = active_config['lakehouse']['volume']
max_workers = active_config['data_download']['max_workers']
```

#### Methods

**`get_active_config()`**
Returns the most recent configuration from the config table:
```python
config = ConfigLoader("my_catalog", "my_schema")
active = config.get_active_config()

# Returns dictionary with structure:
# {
#   'lakehouse': {
#     'catalog': 'catalog_name',
#     'schema': 'schema_name',
#     'volume': 'volume_name'
#   },
#   'data_download': {
#     'max_workers': 64,
#     'max_records': 5000
#   }
# }
```

**`get_config_by_id(config_id)`**
Retrieves a specific configuration by ID:
```python
config = ConfigLoader("my_catalog", "my_schema")
specific = config.get_config_by_id("config_123")
```

#### Configuration Structure

The configuration follows this schema:

```json
{
  "lakehouse": {
    "catalog": "catalog_name",
    "schema": "schema_name",
    "volume": "volume_name"
  },
  "data_download": {
    "max_workers": 64,
    "max_records": 5000
  },
  "cluster": {
    "node_type": "i3.2xlarge",
    "min_workers": 1,
    "max_workers": 4
  }
}
```

#### Integration with Notebooks

The config loader is used by downstream notebooks to access configuration:

**01-data-download.py:**
```python
from config_loader import ConfigLoader

# Load config
config = ConfigLoader(catalog, schema).get_active_config()

# Use config values
VOLUME_PATH = f"/Volumes/{config['lakehouse']['catalog']}/{config['lakehouse']['schema']}/{config['lakehouse']['volume']}"
MAX_WORKERS = config['data_download']['max_workers']
```

**02-tcga-expression-clustering-optimized.py:**
```python
from config_loader import ConfigLoader

# Load config
config = ConfigLoader(catalog, schema).get_active_config()

# Access lakehouse settings
database_name = f"{config['lakehouse']['catalog']}.{config['lakehouse']['schema']}"
```

#### Error Handling

The config loader includes basic error handling:

```python
try:
    config = ConfigLoader(catalog, schema).get_active_config()
except Exception as e:
    print(f"Failed to load configuration: {e}")
    # Fall back to widget parameters or defaults
```

#### Configuration Table

The configuration is stored in a Delta table created by `00-load-config.py`:

- **Table**: `{catalog}.{schema}.config_table`
- **Schema**:
  - `config_id` (string): Unique configuration ID
  - `timestamp` (timestamp): Creation timestamp
  - `config` (struct): Configuration parameters
- **Location**: Unity Catalog managed table

#### Best Practices

1. **Use Config Loader**: Always use `ConfigLoader` instead of reading config table directly
2. **Cache Configuration**: Load once at notebook start, reuse throughout
3. **Handle Missing Config**: Provide fallback values for optional parameters
4. **Validate Values**: Check configuration values before use

#### Example: Complete Notebook Setup

```python
# Databricks notebook source

from config_loader import ConfigLoader

# Get parameters from widgets
dbutils.widgets.text("catalog", "my_catalog", "Catalog")
dbutils.widgets.text("schema", "my_schema", "Schema")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# Load configuration
try:
    config_loader = ConfigLoader(catalog, schema)
    config = config_loader.get_active_config()

    # Extract configuration
    CATALOG = config['lakehouse']['catalog']
    SCHEMA = config['lakehouse']['schema']
    VOLUME = config['lakehouse']['volume']
    MAX_WORKERS = config.get('data_download', {}).get('max_workers', 64)

    print(f"Configuration loaded successfully")
    print(f"Catalog: {CATALOG}, Schema: {SCHEMA}, Volume: {VOLUME}")

except Exception as e:
    print(f"Failed to load configuration: {e}")
    print("Using widget parameters as fallback")
    CATALOG = catalog
    SCHEMA = schema
    VOLUME = "tcga_files"
    MAX_WORKERS = 64

# Continue with notebook logic
volume_path = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
```

## Testing

To test the config loader:

```python
# Check if config table exists
display(spark.sql(f"SHOW TABLES IN {catalog}.{schema} LIKE 'config_table'"))

# Read config table directly
display(spark.table(f"{catalog}.{schema}.config_table"))

# Test config loader
from config_loader import ConfigLoader

config = ConfigLoader("my_catalog", "my_schema")
active = config.get_active_config()
print(active)
```

## Troubleshooting

**Config table not found:**
- Run `00-load-config.py` first to create the config table
- Verify catalog and schema names are correct
- Check Unity Catalog permissions

**Missing configuration fields:**
- Check `config.json` has all required fields
- Verify `00-load-config.py` ran successfully
- Review config table contents

**Import errors:**
- Ensure `utils/` directory is in Python path
- Check `__init__.py` exists in utils directory
- Verify file deployment in bundle artifacts

## Additional Resources

- [Unity Catalog Tables](https://docs.databricks.com/data-governance/unity-catalog/create-tables.html)
- [Databricks Widgets](https://docs.databricks.com/notebooks/widgets.html)
- [Delta Lake](https://docs.delta.io/)
