# Configuration Management

This directory contains configuration files and utilities for the TCGA project.

## Configuration Files

### `config.json` (Production)
Main configuration file for production environment. Located at project root.

### `config.dev.json` (Development)
Configuration for development environment with:
- Smaller data samples (max_records: 1000)
- Fewer workers (32)
- Separate dev catalog/schema

### `config.staging.json` (Staging)
Configuration for staging environment with:
- Medium data samples (max_records: 10000)
- Medium workers (48)
- Separate staging catalog/schema

## Configuration Manager

The `config_manager.py` module provides a robust configuration management system with:

- **Validation**: Ensures all required fields are present and valid
- **Environment Support**: Load different configs for dev/staging/prod
- **Environment Variables**: Override config values with env vars
- **Databricks Integration**: Create widgets and read from secrets
- **Type Safety**: Dataclass-based configuration with type hints

### Usage

#### Basic Usage

```python
from config.config_manager import load_config

# Load default (production) config
config = load_config()

# Access configuration
print(config.lakehouse.volume_path)  # /Volumes/my_catalog/tcga/tcga_files
print(config.api.cases_endpt)  # https://api.gdc.cancer.gov/cases
print(config.pipeline.max_workers)  # 64
```

#### Environment-Specific Config

```python
# Load development config
config = load_config(environment="dev")

# Or specify path explicitly
config = load_config(config_path="./config/config.dev.json")
```

#### Databricks Integration

```python
# In a Databricks notebook
from config.config_manager import load_config

config = load_config()

# Create widgets from config (allows runtime override)
config.get_databricks_widgets(dbutils)

# Access updated values
print(f"Using catalog: {config.lakehouse.catalog}")
print(f"Volume path: {config.lakehouse.volume_path}")
```

#### Export as Dictionary

```python
config = load_config()
config_dict = config.to_dict()

# Use in logging or debugging
import json
print(json.dumps(config_dict, indent=2))
```

## Environment Variables

Override configuration values using environment variables:

```bash
export TCGA_CATALOG=my_catalog
export TCGA_SCHEMA=my_schema
export TCGA_MAX_WORKERS=128
export TCGA_FORCE_DOWNLOAD=true
```

Supported environment variables:
- `TCGA_CATALOG`: Override catalog name
- `TCGA_SCHEMA`: Override schema name
- `TCGA_VOLUME`: Override volume name
- `TCGA_MAX_WORKERS`: Override max concurrent workers
- `TCGA_MAX_RECORDS`: Override max records to download
- `TCGA_FORCE_DOWNLOAD`: Set to 'true' to force re-download

## Configuration Structure

```json
{
  "lakehouse": {
    "catalog": "catalog_name",
    "schema": "schema_name",
    "volume": "volume_name"
  },
  "api_paths": {
    "cases_endpt": "https://api.gdc.cancer.gov/cases",
    "files_endpt": "https://api.gdc.cancer.gov/files",
    "data_endpt": "https://api.gdc.cancer.gov/data/"
  },
  "pipeline": {
    "max_workers": 64,
    "max_records": 20000,
    "force_download": false,
    "retry_attempts": 3,
    "timeout_seconds": 300
  }
}
```

## Validation

The configuration manager performs validation on load:

1. **Lakehouse**: Ensures catalog, schema, and volume are set
2. **API**: Validates all endpoints are valid URLs
3. **Pipeline**: Checks numeric values are in reasonable ranges

If validation fails, a `ValueError` is raised with details.

## Best Practices

1. **Never commit secrets**: Use Databricks secrets for sensitive values
2. **Use environment variables**: For environment-specific overrides
3. **Validate early**: Load and validate config at notebook/script start
4. **Document changes**: Update this README when adding new config fields
5. **Test configs**: Ensure dev/staging configs work before deploying to prod

## Databricks Secrets Integration

For sensitive values, use Databricks secrets:

```python
# In Databricks notebook
from config.config_manager import load_config

config = load_config()

# Override with secret values
try:
    config.lakehouse.catalog = dbutils.secrets.get("tcga", "catalog_name")
except Exception:
    print("Using config file value for catalog")

config.validate()  # Re-validate after changes
```

## Troubleshooting

### "Config file not found"
- Ensure `config.json` exists in project root
- Or specify path explicitly: `load_config(config_path="/path/to/config.json")`

### "Catalog name must be configured"
- Update `config.json` and change `<CHANGE TO YOUR CATALOG NAME>` to actual catalog
- Or set `TCGA_CATALOG` environment variable

### Validation Errors
- Check that all required fields are present in config file
- Ensure numeric values are reasonable (e.g., max_workers > 0)
- Verify API endpoints start with http:// or https://
