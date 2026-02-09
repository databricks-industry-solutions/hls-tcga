# TCGA Pipeline Deployment Guide

This guide explains how to deploy the TCGA data pipeline to your Databricks workspace.

## Prerequisites

1. **Databricks CLI** installed and configured
   ```bash
   pip install databricks-cli
   databricks configure --token
   ```

2. **Unity Catalog** access in your Databricks workspace

3. **Python 3.7+** installed locally

## Quick Start

### 1. Clone the Repository

```bash
git clone <repository-url>
cd hls-tcga
```

### 2. Run Deployment Script

The simplest way to deploy is using the interactive deployment script:

```bash
python deploy.py
```

This will:
- Prompt you for configuration parameters (catalog name, instance types, etc.)
- Create a `config.json` file with your settings
- Validate your Databricks connection
- Deploy the complete pipeline to your workspace

### 3. Run the Pipeline

After deployment, you can run the workflow:

```bash
python deploy.py --run
```

Or use the Databricks CLI directly:

```bash
databricks bundle run tcga_data_workflow --target dev
```

## Deployment Options

### Interactive Deployment (Recommended for First Time)

```bash
python deploy.py
```

The script will:
1. Detect your Databricks profiles and let you choose one
2. Auto-detect cloud provider (AWS/Azure)
3. Prompt for configuration with sensible defaults
4. Create/update `config.json`
5. Deploy the pipeline

### Deploy and Run Immediately

```bash
python deploy.py --run
```

### Configuration Only (No Deployment)

Useful if you just want to create/update `config.json`:

```bash
python deploy.py --config-only
```

### Non-Interactive Deployment

Use existing `config.json` without prompts (fails if config doesn't exist):

```bash
python deploy.py --non-interactive
```

### Deploy Without Running

Deploy the pipeline but don't start the workflow:

```bash
python deploy.py --deploy-only
```

## Configuration Parameters

The `config.json` file contains:

```json
{
  "lakehouse": {
    "catalog": "your_catalog_name",
    "schema": "tcga",
    "volume": "tcga_files"
  },
  "pipeline": {
    "max_workers": 64,
    "max_records": 20000,
    "force_download": false,
    "retry_attempts": 3,
    "timeout_seconds": 300
  },
  "compute": {
    "download_node_type": "r5d.16xlarge",
    "etl_node_type": "r5d.2xlarge",
    "analysis_node_type": "r5d.xlarge"
  },
  "deployment": {
    "profile": "your_databricks_profile",
    "cloud": "aws"
  }
}
```

### Key Parameters

- **catalog**: Unity Catalog name (must exist or you must have permission to create)
- **schema**: Schema name within the catalog (will be created if doesn't exist)
- **volume**: Volume name for raw data storage (will be created if doesn't exist)
- **max_workers**: Concurrent download threads (adjust based on your node size)
- **max_records**: Maximum number of records to download from GDC
- **compute node types**: Instance types for different workloads

### Compute Resources

**AWS Defaults:**
- Download: `r5d.16xlarge` (64 vCPUs, 512 GB RAM) - Single node for concurrent downloads
- ETL: `r5d.2xlarge` (8 vCPUs, 64 GB RAM) - Memory-optimized cluster
- Analysis: `r5d.xlarge` (4 vCPUs, 32 GB RAM) - Autoscaling cluster

**Azure Defaults:**
- Download: `Standard_E64s_v3` (64 vCPUs, 432 GB RAM)
- ETL: `Standard_E8s_v3` (8 vCPUs, 64 GB RAM)
- Analysis: `Standard_E4s_v3` (4 vCPUs, 32 GB RAM)

## Pipeline Components

After deployment, the following resources are created:

### 1. Jobs

**TCGA Data Workflow** (Main pipeline)
- Download data from GDC API
- Run DLT pipeline for transformation
- Perform gene expression clustering analysis

**TCGA Incremental Refresh** (Weekly updates)
- Download only new data
- Incremental DLT pipeline refresh

### 2. Delta Live Tables Pipeline

**TCGA ETL Pipeline**
- Bronze layer: Raw data ingestion
- Silver layer: Cleaned and transformed data
- Data quality checks with expectations

### 3. Tables Created

All tables are stored in: `{catalog}.{schema}`

- `cases_demographics` - Patient demographic information
- `cases_diagnoses` - Diagnosis details
- `cases_exposures` - Exposure history
- `expression_profiles` - Gene expression data
- `expression_summary` - Aggregated expression metrics
- `expression_clustering_results` - ML clustering results

## Monitoring and Management

### View Deployment Status

```bash
databricks bundle summary --target dev
```

### Check Job Runs

```bash
databricks jobs list-runs --limit 10
```

### Access Databricks UI

Your deployment script will display URLs for:
- Job workflows
- DLT pipeline
- Run monitoring

## Troubleshooting

### Common Issues

1. **Catalog not accessible**
   - Ensure you have USE CATALOG permission
   - Or create the catalog first: `CREATE CATALOG IF NOT EXISTS <catalog_name>`

2. **Instance type not available**
   - Check your workspace's available instance types
   - Update `config.json` with supported instance types
   - Run `python deploy.py` again

3. **Authentication errors**
   - Verify Databricks CLI is configured: `databricks auth profiles`
   - Re-authenticate: `databricks configure --token`

4. **Bundle validation failed**
   - Check `databricks.yml` syntax
   - Ensure all referenced files exist
   - Run: `databricks bundle validate --target dev`

### Getting Help

- Check logs in Databricks UI
- Review job run output
- Validate configuration: `python deploy.py --config-only`

## Advanced Configuration

### Multiple Environments

To set up staging or production:

1. Create environment-specific configs:
   ```bash
   cp config.json config.staging.json
   # Edit config.staging.json
   ```

2. Add target to `databricks.yml`:
   ```yaml
   targets:
     staging:
       mode: production
       workspace:
         profile: PRODUCTION
   ```

3. Deploy to specific environment:
   ```bash
   python deploy.py --config config.staging.json
   databricks bundle deploy --target staging
   ```

### Custom Data Filters

Edit pipeline notebooks to add filters:
- `01-data-download.py` - Modify GDC API filters
- `etl_pipelines/transformations/data_ingestion.py` - Add DLT expectations

### Scaling

Adjust cluster sizes in `config.json`:
- Increase `max_workers` for faster downloads
- Use larger instance types for bigger datasets
- Adjust `max_records` to control data volume

## Pipeline Workflow

```
┌─────────────────┐
│  deploy.py      │  Creates config, validates, deploys
└────────┬────────┘
         │
         v
┌─────────────────┐
│ Download Data   │  Single node (64 cores), concurrent downloads
└────────┬────────┘
         │
         v
┌─────────────────┐
│ DLT Pipeline    │  Memory-optimized cluster, data validation
└────────┬────────┘
         │
         v
┌─────────────────┐
│ ML Analysis     │  Clustering, dimensionality reduction
└─────────────────┘
```

## Data Flow

```
GDC API → Volume → Bronze Tables → Silver Tables → Analysis Results
```

## Next Steps

After successful deployment:

1. **Monitor first run** - Initial data download may take several hours
2. **Validate data** - Check table row counts and data quality
3. **Schedule jobs** - Enable job schedules in Databricks UI
4. **Set up alerts** - Configure email notifications for failures
5. **Review results** - Explore clustering results and visualizations

## Support

For issues or questions:
- GitHub Issues: [Repository URL]
- Documentation: See individual notebook headers
- Databricks Support: Contact your workspace admin
