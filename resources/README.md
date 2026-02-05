# Databricks Asset Bundle Resources

This directory contains resource definitions for the TCGA project Databricks Asset Bundle (DAB).

## Resource Files

### `pipelines.yml`
Defines Delta Live Tables (DLT) pipelines for data ingestion and transformation:
- **tcga_etl_pipeline**: Main ETL pipeline with bronze and silver layers

### `jobs.yml`
Defines Databricks jobs for workflow orchestration:
- **tcga_data_workflow**: Complete end-to-end workflow (setup → download → ETL → analysis)
- **tcga_incremental_refresh**: Incremental data refresh for periodic updates

## Deployment

### Prerequisites

1. **Install Databricks CLI**:
   ```bash
   pip install databricks-cli
   ```

2. **Configure authentication**:
   ```bash
   databricks configure --token
   ```
   Or use environment variables:
   ```bash
   export DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
   export DATABRICKS_TOKEN=your-token
   ```

3. **Set required environment variables**:
   ```bash
   export DATABRICKS_USER_EMAIL=your-email@company.com
   export DATABRICKS_SERVICE_PRINCIPAL=sp-name  # For production
   ```

### Deploy to Development

```bash
# Deploy all resources
databricks bundle deploy --target dev

# Deploy and run the main workflow
databricks bundle run tcga_data_workflow --target dev
```

### Deploy to Staging

```bash
# Deploy to staging
databricks bundle deploy --target staging

# Validate the deployment
databricks bundle validate --target staging

# Run the workflow
databricks bundle run tcga_data_workflow --target staging
```

### Deploy to Production

```bash
# Deploy to production (requires appropriate permissions)
databricks bundle deploy --target prod

# Start the DLT pipeline
databricks pipelines start --pipeline-id $(databricks bundle resources pipelines tcga_etl_pipeline --target prod)

# Run the main workflow (if not scheduled)
databricks bundle run tcga_data_workflow --target prod
```

## Resource Definitions

### DLT Pipeline Configuration

The DLT pipeline includes:
- **Target**: `{catalog}.{schema}` from variables
- **Libraries**: Both ingestion and transformation notebooks
- **Cluster**: Autoscaling 1-4 workers with Photon enabled
- **Edition**: Advanced (includes expectations and data quality)
- **Continuous**: Disabled (batch mode)
- **Storage**: `/mnt/dlt/{environment}/tcga_pipeline`

### Job Configuration

The main workflow job includes 4 tasks:
1. **Setup**: Initialize Unity Catalog resources
2. **Download Data**: Fetch data from GDC API
3. **Run DLT Pipeline**: Execute ETL transformations
4. **Expression Clustering**: Run analysis notebook

#### Job Features:
- Task dependencies ensure proper execution order
- Retry logic on timeout for data download
- Email notifications on start/success/failure
- Configurable parallelism via `max_workers` variable
- 8-hour timeout for complete workflow

### Scheduling

Jobs can be scheduled using cron expressions:
- **tcga_data_workflow**: Daily at 2 AM (paused by default)
- **tcga_incremental_refresh**: Weekly on Sunday at 3 AM (paused by default)

Enable schedule in Databricks UI or via CLI:
```bash
databricks jobs update --job-id <job-id> --pause-status UNPAUSED
```

## Environment Variables

### Required
- `DATABRICKS_HOST`: Workspace URL
- `DATABRICKS_TOKEN`: Personal access token or service principal token

### Optional
- `DATABRICKS_USER_EMAIL`: Email for job notifications
- `DATABRICKS_SERVICE_PRINCIPAL`: Service principal name for production runs

## Customization

### Modifying Cluster Configuration

Edit `cluster_node_type` in `databricks.yml`:
```yaml
variables:
  cluster_node_type:
    default: "i3.2xlarge"  # AWS
    # default: "Standard_DS3_v2"  # Azure
    # default: "n1-standard-4"  # GCP
```

### Adjusting Worker Count

Modify `max_workers` per environment:
```yaml
targets:
  dev:
    variables:
      max_workers: 32  # Fewer workers in dev
  prod:
    variables:
      max_workers: 128  # More workers in prod
```

### Adding New Tasks

Add to `tasks` array in `jobs.yml`:
```yaml
- task_key: "new_task"
  depends_on:
    - task_key: "previous_task"
  job_cluster_key: "analysis_cluster"
  notebook_task:
    notebook_path: ./new-notebook.py
  timeout_seconds: 3600
```

## Monitoring

### View Pipeline Status
```bash
# Get pipeline ID
databricks bundle resources pipelines tcga_etl_pipeline --target prod

# View pipeline status
databricks pipelines get --pipeline-id <pipeline-id>

# View recent updates
databricks pipelines list-updates --pipeline-id <pipeline-id>
```

### View Job Runs
```bash
# List recent runs
databricks jobs list-runs --job-id <job-id> --limit 10

# Get run details
databricks jobs get-run --run-id <run-id>

# View run output
databricks jobs get-run-output --run-id <run-id>
```

## Troubleshooting

### Bundle Validation Errors

```bash
# Validate bundle configuration
databricks bundle validate --target dev

# Common issues:
# - Missing environment variables
# - Invalid resource references
# - Syntax errors in YAML
```

### Deployment Failures

1. Check workspace connectivity:
   ```bash
   databricks workspace ls /
   ```

2. Verify permissions:
   ```bash
   databricks workspace get-status ~/.bundle
   ```

3. View detailed error logs in Databricks UI

### Pipeline Failures

1. Check DLT event log in pipeline UI
2. Review data quality metrics
3. Verify volume paths and permissions
4. Check for schema mismatches

### Job Failures

1. View run details in Jobs UI
2. Check task-specific logs
3. Verify cluster configuration
4. Review task dependencies

## Best Practices

1. **Use Development Target First**: Always test in dev before deploying to prod
2. **Service Principals**: Use service principals for production deployments
3. **Version Control**: Commit bundle configuration changes to git
4. **Incremental Deployment**: Use `databricks bundle deploy` for iterative development
5. **Monitor Costs**: Review cluster utilization and adjust autoscaling settings
6. **Schedule Wisely**: Set appropriate schedules based on data refresh requirements
7. **Test Notifications**: Verify email notifications work before production deployment

## Additional Resources

- [Databricks Asset Bundles Documentation](https://docs.databricks.com/dev-tools/bundles/index.html)
- [DLT Configuration Reference](https://docs.databricks.com/workflows/delta-live-tables/index.html)
- [Jobs API Reference](https://docs.databricks.com/api/workspace/jobs)
