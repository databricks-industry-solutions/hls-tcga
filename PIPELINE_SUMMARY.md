# TCGA Pipeline - Production Summary

## ✅ Status: Production Ready

All components tested and working. Pipeline successfully processes TCGA data end-to-end.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                  TCGA Data Workflow                          │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  1️⃣  load_config                                            │
│      • Loads configuration from config.json                 │
│      • Stores in Delta table with versioning               │
│      • Cluster: Single node r5d.16xlarge                    │
│      • Timeout: 15 minutes                                  │
│                                                              │
│  2️⃣  download_data                                          │
│      • Downloads TCGA files from GDC API                    │
│      • Respects force_download flag                         │
│      • Skips existing files when force_download=false       │
│      • Parallel downloads (64 workers)                      │
│      • Cluster: Single node r5d.16xlarge (64 cores)         │
│      • Timeout: 4 hours                                     │
│                                                              │
│  3️⃣  create_delta_tables                                    │
│      • Creates Delta tables from downloaded files           │
│      • Tables: expression_files_info, cases,                │
│        expression_profiles                                  │
│      • Cluster: Single node r5d.16xlarge                    │
│      • Timeout: 30 minutes                                  │
│                                                              │
│  4️⃣  expression_clustering                                  │
│      • PCA dimensionality reduction                         │
│      • t-SNE visualization                                  │
│      • Clustering analysis                                  │
│      • MLflow experiment tracking                           │
│      • Cluster: Auto-scale r5d.xlarge (2-6 workers)         │
│      • Timeout: 2 hours                                     │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

## Delta Tables

### 1. pipeline_config
**Location**: `{catalog}.{schema}.pipeline_config`
**Purpose**: Configuration versioning and audit trail

| Column | Type | Description |
|--------|------|-------------|
| config_id | string | Unique identifier (UUID) |
| config_timestamp | timestamp | When config was created |
| config_json | string | Full configuration as JSON |
| catalog | string | Unity Catalog name |
| schema | string | Schema name |
| volume | string | Volume name |

### 2. expression_files_info
**Location**: `{catalog}.{schema}.expression_files_info`
**Purpose**: Metadata about gene expression files

| Column | Type | Description |
|--------|------|-------------|
| file_id | string | Unique file identifier |
| case_id | string | Patient case identifier |
| file_size | long | File size in bytes |
| data_format | string | File format (TSV) |
| ingestion_timestamp | timestamp | When data was ingested |

### 3. cases
**Location**: `{catalog}.{schema}.cases`
**Purpose**: Clinical case information

| Column | Type | Description |
|--------|------|-------------|
| case_id | string | Unique case identifier |
| demographic.* | struct | Demographics (age, gender, race) |
| diagnoses.* | array | Diagnosis information |
| exposures.* | array | Exposure information |
| ingestion_timestamp | timestamp | When data was ingested |

### 4. expression_profiles
**Location**: `{catalog}.{schema}.expression_profiles`
**Purpose**: Gene expression data (FPKM values)

| Column | Type | Description |
|--------|------|-------------|
| gene_id | string | Gene identifier (ENSG...) |
| gene_name | string | Gene symbol |
| gene_type | string | Gene type |
| fpkm_unstranded | double | FPKM expression value |
| tpm_unstranded | double | TPM expression value |
| file_id | string | Source file identifier |
| ingestion_timestamp | timestamp | When data was ingested |

## Configuration

### Key Parameters (config.json)

```json
{
  "lakehouse": {
    "catalog": "kermany",
    "schema": "tcga",
    "volume": "tcga_files"
  },
  "pipeline": {
    "max_workers": 64,
    "max_records": 10000,
    "force_download": false,
    "retry_attempts": 3,
    "timeout_seconds": 60
  }
}
```

### Important Settings

**force_download**
- `false` (default) - Only download new files, skip existing
- `true` - Re-download all files (full refresh)

**max_records**
- Limits number of files to download
- Use for testing or partial loads
- Set to large number (e.g., 100000) for full dataset

**max_workers**
- Number of parallel download threads
- Default: 64 (optimized for r5d.16xlarge)
- Adjust based on cluster size

## Running the Pipeline

### Full Pipeline

```bash
databricks jobs run-now <job-id> --profile HLS
```

### Individual Tasks

Run specific notebooks in Databricks UI:
1. `00-setup.ipynb` - Initial setup (one-time)
2. `00-load-config.py` - Load configuration
3. `01-data-download.py` - Download data
4. `03-create-delta-tables.py` - Create tables
5. `02-tcga-expression-clustering-optimized.py` - Analysis

### Incremental Updates

For incremental data updates:

1. Set `force_download: false` in config.json
2. Run the pipeline
3. Only new files will be downloaded
4. Existing tables will be refreshed

## Monitoring

### Job Runs
- View in Databricks Jobs UI
- Each task shows status: SUCCESS/FAILED/RUNNING
- Click task for detailed logs

### Data Quality
- Check row counts: `SELECT COUNT(*) FROM {catalog}.{schema}.expression_profiles`
- Verify timestamps: `SELECT MAX(ingestion_timestamp) FROM {catalog}.{schema}.cases`

### MLflow Experiments
- Expression clustering results tracked in MLflow
- View experiments in Databricks ML UI
- Metrics: PCA variance, clustering scores

## Troubleshooting

### Pipeline Fails at download_data
**Symptoms**: Download task fails, no files in volume
**Solutions**:
- Check GDC API connectivity
- Verify Unity Catalog volume permissions
- Check network/firewall rules

### Pipeline Fails at create_delta_tables
**Symptoms**: "Not a Delta table" error
**Solutions**:
- Tables already exist as non-Delta
- Drop tables: `DROP TABLE IF EXISTS {catalog}.{schema}.{table_name}`
- Or use `03-create-delta-tables.py` which handles this automatically

### No New Files Downloaded
**Symptoms**: download_data succeeds but no new data
**Cause**: `force_download=false` and all files already exist
**Solution**: Expected behavior for incremental loads

### Expression Clustering Fails
**Symptoms**: Analysis task fails with memory error
**Solutions**:
- Use sampling: set `use_sample: "true"` in job parameters
- Reduce `n_top_genes` parameter
- Increase cluster size (more workers or larger instance)

## Performance

### Expected Runtime (10K records)
- load_config: ~2 minutes
- download_data: ~30-60 minutes (depends on network)
- create_delta_tables: ~10-15 minutes
- expression_clustering: ~30-45 minutes

**Total**: ~1.5-2 hours

### Optimization Tips
1. Use larger `max_workers` for faster downloads
2. Enable `use_sample: true` for faster analysis during development
3. Schedule during off-peak hours for better resource availability

## Maintenance

### Regular Tasks
1. Monitor volume usage: `dbutils.fs.du('/Volumes/{catalog}/{schema}/{volume}')`
2. Review failed runs in Jobs UI
3. Check MLflow experiments for analysis quality
4. Optimize Delta tables: `OPTIMIZE {catalog}.{schema}.{table_name}`

### Updates
1. Pull latest code: `git pull`
2. Review changes: `git log`
3. Deploy: `databricks bundle deploy --profile HLS`
4. Test with sample data before full run

## Security

### Access Control
- Unity Catalog manages all permissions
- Tables inherit catalog/schema permissions
- Volume access controlled by UC grants

### Data Governance
- All data stored in Unity Catalog
- Full audit trail via `ingestion_timestamp`
- Configuration versioning in `pipeline_config` table

## Support

For issues:
1. Check Databricks job logs
2. Review this documentation
3. Contact Databricks admin
4. File GitHub issue (if applicable)

---

**Last Updated**: 2026-02-10
**Status**: ✅ Production Ready
**Version**: 1.0
