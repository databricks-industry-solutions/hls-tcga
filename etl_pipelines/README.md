# TCGA ETL Pipelines

This directory contains the Delta Live Tables (DLT) pipeline for transforming TCGA data from bronze to silver layers.

## Pipeline Architecture

### Bronze Layer (Non-DLT)
Created by `03-create-bronze-layer.py`:
- `expression_files_info_bronze`: Raw file metadata
- `cases_bronze`: Raw clinical case information
- `expression_profiles_bronze`: Raw gene expression data

### Silver Layer (DLT: `transformations/transform.py`)
Transforms bronze tables with data quality checks:

**Core Tables:**
- `expression_files_info`: File metadata with validation
- `cases`: Clinical case information
- `expression_profiles`: Gene expression with FPKM validation

**Clinical Tables:**
- `cases_demographics`: Patient demographics with vital status
- `cases_diagnoses`: Tumor classification and treatments
- `cases_exposures`: Environmental exposures with derived smoking status

## Data Quality Expectations

Each table includes DLT expectations:

### Validation (`@dlt.expect`)
Logs warnings but keeps records:
```python
@dlt.expect("valid_fpkm_values", "fpkm_unstranded >= 0 OR fpkm_unstranded IS NULL")
```

### Drop (`@dlt.expect_or_drop`)
Drops invalid records:
```python
@dlt.expect_or_drop("valid_case_id", "case_id IS NOT NULL")
@dlt.expect_or_drop("valid_file_id", "file_id IS NOT NULL")
```

## Running the Pipeline

### Using deploy.py (Recommended)

```bash
python deploy.py --run
```

### Using Databricks Bundle

```bash
databricks bundle deploy
databricks bundle run tcga_data_workflow
```

### Manual DLT Execution

The DLT pipeline runs automatically as part of the main workflow, or manually:

```bash
# Get pipeline ID
databricks pipelines list | grep "TCGA ETL Pipeline"

# Start pipeline
databricks pipelines start --pipeline-id <pipeline-id>
```

## Configuration

### Serverless DLT
- **Compute**: Serverless (Unity Catalog enabled)
- **Target**: `{catalog}.{schema}` from config.json
- **Edition**: Advanced (includes expectations)
- **Mode**: Batch (not continuous)

### Quality Checks

**All Tables:**
- Non-null case_id and file_id
- Transformation timestamps

**Specific Validations:**
- Gender: 'male', 'female', or NULL
- Year of birth: 1900-2020 or NULL
- Cigarettes per day: >= 0 or NULL
- FPKM values: >= 0 or NULL

## Features

### Change Data Feed (CDF)
All silver tables have CDF enabled:
```python
table_properties={
    "delta.enableChangeDataFeed": "true"
}
```

### Auto-Optimization
Z-ordering on key columns:
```python
table_properties={
    "pipelines.autoOptimize.zOrderCols": "case_id,file_id"
}
```

### Derived Columns

**cases_demographics:**
- `is_deceased`: Boolean based on year_of_death

**cases_diagnoses:**
- `has_treatment`: Boolean based on treatment IDs

**cases_exposures:**
- `smoking_status`: Categorized (Current/Former Smoker, Non-Smoker, Unknown)

## Monitoring

### DLT Metrics Dashboard
Available in Databricks UI:
- Pipeline run history and status
- Data quality metrics (expectations pass/fail)
- Row counts and data lineage
- Processing duration

### Key Metrics to Monitor

1. **Expectation Violations**: Records dropped due to quality issues
2. **Row Counts**: Verify expected data volume
3. **Processing Time**: Monitor for performance degradation
4. **Error Rates**: Track pipeline failures

## Troubleshooting

### Common Issues

**Pipeline Fails to Start**
- Verify serverless is enabled in workspace
- Check Unity Catalog permissions
- Ensure bronze tables exist

**Table Not Found**
- Run bronze layer first: `03-create-bronze-layer.py`
- Verify catalog and schema names in config.json
- Check table naming (should have `_bronze` suffix for source tables)

**Quality Expectation Failures**
- Review DLT expectations metrics in UI
- Check source data quality in bronze tables
- Adjust expectations if legitimate data patterns exist

**Performance Issues**
- Serverless automatically scales
- Check for data skew in large tables
- Review Z-ordering configuration

### Debugging Steps

1. **Check Bronze Tables Exist:**
   ```sql
   SHOW TABLES IN {catalog}.{schema} LIKE '*_bronze'
   ```

2. **Verify DLT Pipeline Config:**
   ```bash
   databricks pipelines get <pipeline-id>
   ```

3. **Review Event Logs:**
   - Navigate to pipeline in Databricks UI
   - Check "Event Log" tab for detailed errors

4. **Test Queries:**
   ```sql
   SELECT COUNT(*) FROM {catalog}.{schema}.cases_bronze;
   SELECT COUNT(*) FROM {catalog}.{schema}.cases;
   ```

## Next Steps

After pipeline completion:
1. **Verify Quality**: Review DLT metrics dashboard
2. **Run Analysis**: Execute `02-tcga-expression-clustering-optimized.py`
3. **Create Dashboards**: Use silver tables for visualization
4. **Schedule Updates**: Enable incremental refresh workflow

## Additional Resources

- [Delta Live Tables Documentation](https://docs.databricks.com/delta-live-tables/)
- [Data Quality Expectations](https://docs.databricks.com/delta-live-tables/expectations.html)
- [Unity Catalog with DLT](https://docs.databricks.com/delta-live-tables/unity-catalog.html)
