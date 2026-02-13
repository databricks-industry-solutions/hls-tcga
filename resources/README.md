# Databricks Asset Bundle Resources

This directory contains resource definitions for the TCGA project Databricks Asset Bundle (DAB).

## Resource Files

### `pipelines.yml`
Defines Delta Live Tables (DLT) pipeline for data transformation:
- **tcga_etl_pipeline**: Serverless DLT pipeline with Unity Catalog
  - Transforms bronze (raw) tables into silver (cleaned) tables
  - Includes data quality expectations and validations
  - Creates 6 silver tables with clinical and genomic data

### `jobs.yml`
Defines Databricks jobs for workflow orchestration:
- **tcga_data_workflow**: Main one-time setup workflow
  - load_config → download_data → create_bronze_layer → etl_pipeline (DLT) → expression_clustering
- **tcga_incremental_refresh**: Periodic incremental updates

## Deployment

### Quick Start

```bash
# Recommended: Use deploy.py
python deploy.py

# Or use bundle commands directly
databricks bundle deploy
databricks bundle run tcga_data_workflow
```

### Prerequisites

1. **Databricks CLI configured**:
   ```bash
   databricks configure --token
   ```

2. **Configuration file**:
   Create `config.json` with your settings (or run `python deploy.py --config-only`)

### Deploy Commands

```bash
# Deploy to development (default)
databricks bundle deploy

# Deploy and run
databricks bundle run tcga_data_workflow

# Deploy to specific target
databricks bundle deploy --target dev
```

## Pipeline Architecture

### Main Workflow Tasks

1. **load_config**: Loads config.json into Delta table
2. **download_data**: Fetches TCGA data from GDC API (64 parallel workers)
3. **create_bronze_layer**: Creates raw Delta tables (_bronze suffix)
4. **etl_pipeline**: Serverless DLT pipeline transforms bronze → silver
5. **expression_clustering**: ML analysis with MLflow tracking

### DLT Pipeline (Serverless)

- **Compute**: Serverless (no cluster management)
- **Unity Catalog**: Enabled by default
- **Target**: `{catalog}.{schema}`
- **Edition**: Advanced (includes data quality)
- **Mode**: Batch (not continuous)

### Silver Tables Created

1. **expression_files_info**: File metadata with validation
2. **cases**: Clinical case information
3. **expression_profiles**: Gene expression with FPKM validation
4. **cases_demographics**: Patient demographics
5. **cases_diagnoses**: Tumor classification and treatments
6. **cases_exposures**: Environmental exposures

## Configuration

### Runtime Parameters

Configure via `config.json` or bundle variables:

```yaml
variables:
  catalog_name: "my_catalog"
  schema_name: "tcga"
  volume_name: "tcga_files"
  user_name: "user@company.com"
  max_workers: 64
```

### Cluster Configuration

Job clusters use on-demand instances (no spot termination):
- **download_cluster**: r5d.16xlarge (64 vCPUs, 512 GB RAM) - Single node
- **etl_cluster**: r5d.2xlarge (8 vCPUs, 64 GB RAM) - Autoscale 2-8 workers
- **analysis_cluster**: r5d.xlarge (4 vCPUs, 32 GB RAM) - Autoscale 2-6 workers

## Monitoring

### Check Workflow Status

```bash
# List recent runs
databricks jobs list-runs --job-id <job-id>

# Get run details
databricks jobs get-run --run-id <run-id>
```

### Check DLT Pipeline

```bash
# Get pipeline ID from bundle
databricks pipelines get <pipeline-id>

# View recent updates
databricks pipelines list-updates <pipeline-id>
```

### Logs

- **Job logs**: Available in Databricks Jobs UI
- **DLT logs**: Available in DLT Pipeline UI
- **Task outputs**: Check individual task run pages

## Scheduling

Jobs are created with schedules (paused by default):
- **tcga_data_workflow**: Daily at 2 AM (one-time use recommended)
- **tcga_incremental_refresh**: Weekly on Sundays at 3 AM

Enable in Databricks UI or via CLI:
```bash
databricks jobs update --job-id <job-id> --pause-status UNPAUSED
```

## Troubleshooting

### Common Issues

**DLT Pipeline Fails**
- Check Unity Catalog permissions
- Verify bronze tables exist
- Review DLT event logs in UI

**Bronze Layer Timeout**
- Increase `timeout_seconds` in jobs.yml (default: 7200s)
- Check volume write permissions
- Verify data file sizes

**Download Failures**
- Check GDC API connectivity
- Verify volume exists and is accessible
- Review max_workers setting

### Validation

```bash
# Validate bundle before deploy
databricks bundle validate

# Check for errors
databricks bundle deploy --dry-run
```

## Best Practices

1. **Test in Dev First**: Always validate in development
2. **Use deploy.py**: Simplifies configuration and deployment
3. **Monitor Costs**: Review serverless DLT usage
4. **Check Quality Metrics**: Review DLT expectations in UI
5. **Version Control**: Commit bundle config changes

## Additional Resources

- [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/)
- [Delta Live Tables](https://docs.databricks.com/delta-live-tables/)
- [Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/)
