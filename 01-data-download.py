# Databricks notebook source
# MAGIC %md
# MAGIC # Download Data from Genomic Data Common (GDC)
# MAGIC
# MAGIC In this notebook, we will:
# MAGIC
# MAGIC - Download clinical information from GDC.
# MAGIC - Fetch the corresponding gene expression profiles.
# MAGIC - Persist the files in a specified cloud path for future access.
# MAGIC
# MAGIC [![](https://mermaid.ink/img/pako:eNplkk1vozAQhv-KNauIC434DMGHlQKEaA8rRdqqh4YeXBgSS2AjY7TJRvnva4jSpK0Ptmfmee2xZ85QygqBwmx2LgQhXHBNybQlxNIHbNGixHpnPVr2o_eFKc7eG-ytD9yEOsVbpk6pbKQadT8CL47Xzk16J57xqO9UXdffkUSqCtUdilLHjAeu4QLvYSfwwzB7CPdYSlF9yma5Sld5_sBoVJp_QpLV2s1T60pcxsVMl9msEHvFugN5TgqR7DL5VzSSVSQ1SfCSNeSXqKVqmeZSkFrJlmyy9K0Q6S5HXR7IBgWS9bFT2PcjslWy5ub3RoQ8Pf0k2W6Lque9JvnoN4UgjLzIZmjRMMmVKQTYsFe8AqrVgDa0aO4cTZhqUMBUmwKo2VZYs6HRBZj8jaxj4lXK9qZUctgfgNas6Y01dBXTmHFmHtl-8a4rrqW66dig5Z-TKG82TtHf1yaaesmcjWKq3CA00IU73Q30DEegfujNXScMY88LI9ePYxtOQGN3HnhRFEROsPSWkeNdbPg3JevMIydeRIvYc32zBKF_-Q956dPG?type=png)](https://mermaid.live/edit#pako:eNplkk1vozAQhv-KNauIC434DMGHlQKEaA8rRdqqh4YeXBgSS2AjY7TJRvnva4jSpK0Ptmfmee2xZ85QygqBwmx2LgQhXHBNybQlxNIHbNGixHpnPVr2o_eFKc7eG-ytD9yEOsVbpk6pbKQadT8CL47Xzk16J57xqO9UXdffkUSqCtUdilLHjAeu4QLvYSfwwzB7CPdYSlF9yma5Sld5_sBoVJp_QpLV2s1T60pcxsVMl9msEHvFugN5TgqR7DL5VzSSVSQ1SfCSNeSXqKVqmeZSkFrJlmyy9K0Q6S5HXR7IBgWS9bFT2PcjslWy5ub3RoQ8Pf0k2W6Lque9JvnoN4UgjLzIZmjRMMmVKQTYsFe8AqrVgDa0aO4cTZhqUMBUmwKo2VZYs6HRBZj8jaxj4lXK9qZUctgfgNas6Y01dBXTmHFmHtl-8a4rrqW66dig5Z-TKG82TtHf1yaaesmcjWKq3CA00IU73Q30DEegfujNXScMY88LI9ePYxtOQGN3HnhRFEROsPSWkeNdbPg3JevMIydeRIvYc32zBKF_-Q956dPG)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Initial configurations

# COMMAND ----------

# Import required libraries
import os
import json
import logging
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple, Optional, Dict, Any
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# COMMAND ----------

# MAGIC %run ./00-setup

# COMMAND ----------

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# COMMAND ----------

# Configure widgets for parameterization
dbutils.widgets.text("force_download", "false", "Force Download")
dbutils.widgets.text("max_workers", "64", "Max Concurrent Workers")
dbutils.widgets.text("max_records", "20000", "Max Records to Download")

FORCE_DOWNLOAD = dbutils.widgets.get("force_download").lower() == "true"
MAX_WORKERS = int(dbutils.widgets.get("max_workers"))
MAX_RECORDS = int(dbutils.widgets.get("max_records"))

logger.info(f"Configuration: FORCE_DOWNLOAD={FORCE_DOWNLOAD}, MAX_WORKERS={MAX_WORKERS}, MAX_RECORDS={MAX_RECORDS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Helper Functions

# COMMAND ----------

def get_requests_session() -> requests.Session:
    """
    Create a requests session with retry logic and connection pooling.

    Returns:
        requests.Session: Configured session with retry logic
    """
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=100, pool_maxsize=100)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

# COMMAND ----------

def download_table(
    end_point: str,
    fields: List[str],
    output_path: str,
    size: int = 100000,
    filters: Optional[List[Tuple]] = None
) -> None:
    """
    Download data from GDC API endpoint and save to Unity Catalog volume.

    Args:
        end_point: GDC API endpoint URL
        fields: List of fields to retrieve
        output_path: Volume path to save the downloaded file
        size: Maximum number of records to retrieve
        filters: Optional list of filters as (field, operator, value) tuples

    Raises:
        requests.exceptions.RequestException: If API request fails
    """
    try:
        logger.info(f"Downloading data from {end_point}")

        # Build API filter
        api_filter = None
        if filters:
            _filters = [
                {'op': str(m[1]), 'content': {'field': str(m[0]), 'value': m[2]}}
                for m in filters
            ]
            api_filter = {"op": "and", "content": _filters}

        # Prepare request parameters
        params = {
            "filters": api_filter,
            "fields": ','.join(fields),
            "format": "TSV",
            "size": size
        }

        # Make API request with retry logic
        session = get_requests_session()
        response = session.post(end_point, json=params, timeout=300)
        response.raise_for_status()

        # Write to volume using dbutils
        content = response.content.decode("utf-8")
        dbutils.fs.put(output_path, content, overwrite=True)

        logger.info(f"Successfully downloaded data to {output_path}")

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to download data from {end_point}: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during download: {str(e)}")
        raise

# COMMAND ----------

def download_single_expression(
    uuid: str,
    target_directory_path: str,
    session: requests.Session
) -> Tuple[str, bool, Optional[str]]:
    """
    Download a single gene expression file.

    Args:
        uuid: File UUID to download
        target_directory_path: Volume directory path
        session: Requests session with retry logic

    Returns:
        Tuple of (uuid, success_flag, error_message)
    """
    try:
        file_path = f"{target_directory_path}/{uuid}"
        url = f'{data_endpt}{uuid}'

        response = session.get(url, timeout=60)
        response.raise_for_status()

        content = response.content.decode("utf-8")
        dbutils.fs.put(file_path, content, overwrite=True)

        return (uuid, True, None)
    except Exception as e:
        error_msg = f"Failed to download {uuid}: {str(e)}"
        logger.warning(error_msg)
        return (uuid, False, error_msg)

def download_expressions(
    target_directory_path: str,
    uuids: List[str],
    n_workers: int = 64
) -> Dict[str, Any]:
    """
    Download multiple gene expression files in parallel.

    Args:
        target_directory_path: Volume directory path
        uuids: List of file UUIDs to download
        n_workers: Number of concurrent workers

    Returns:
        Dictionary with download statistics
    """
    logger.info(f"Starting download of {len(uuids)} expression files with {n_workers} workers")

    session = get_requests_session()
    successful = 0
    failed = 0
    errors = []

    with ThreadPoolExecutor(max_workers=n_workers) as executor:
        futures = {
            executor.submit(download_single_expression, uuid, target_directory_path, session): uuid
            for uuid in uuids
        }

        for future in as_completed(futures):
            uuid, success, error = future.result()
            if success:
                successful += 1
                if successful % 100 == 0:
                    logger.info(f"Progress: {successful}/{len(uuids)} files downloaded")
            else:
                failed += 1
                errors.append(error)

    logger.info(f"Download complete: {successful} successful, {failed} failed")

    return {
        "total": len(uuids),
        "successful": successful,
        "failed": failed,
        "errors": errors
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Download Expression Files Metadata

# COMMAND ----------

# DBTITLE 1,Download files list
file_fields = [
    "access",
    "data_format",
    "data_type",
    "file_id",
    "cases.project.project_id",
    "data_category",
    "experimental_strategy",
    "file_name",
    "file_size",
    "type",
    "cases.case_id"
]

files_filters = [
    ('data_format', 'in', ['TSV']),
    ('data_type', 'in', ['Gene Expression Quantification']),
    ('cases.project.program.name', 'in', ['TCGA']),
    ('access', 'in', ['open'])
]

expressions_info_path = f"{volume_path}/expressions_info.tsv"

# Check if file exists using dbutils
try:
    file_exists = len(dbutils.fs.ls(expressions_info_path)) > 0
except Exception:
    file_exists = False

if not file_exists or FORCE_DOWNLOAD:
    logger.info(f'Downloading expressions_info.tsv to {expressions_info_path}')
    download_table(
        files_endpt,
        file_fields,
        expressions_info_path,
        size=MAX_RECORDS,
        filters=files_filters
    )
else:
    logger.info(f'File {expressions_info_path} already exists')

# Read using Spark for better performance
files_list_df = spark.read.csv(expressions_info_path, sep='\t', header=True, inferSchema=True)
record_count = files_list_df.count()
logger.info(f'Loaded {record_count} expression file records')
display(files_list_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Download Clinical Cases Data

# COMMAND ----------

# DBTITLE 1,Download cases with clinical metadata
demographic_fields = [
    "demographic.ethnicity",
    "demographic.gender",
    "demographic.race",
    "demographic.year_of_birth",
    "demographic.year_of_death"
]

diagnoses_fields = [
    "diagnoses.classification_of_tumor",
    "diagnoses.diagnosis_id",
    "diagnoses.primary_diagnosis",
    "diagnoses.tissue_or_organ_of_origin",
    "diagnoses.tumor_grade",
    "diagnoses.tumor_stage",
    "diagnoses.treatments.therapeutic_agents",
    "diagnoses.treatments.treatment_id",
    "diagnoses.treatments.updated_datetime"
]

exposures_fields = [
    "exposures.alcohol_history",
    "exposures.alcohol_intensity",
    "exposures.bmi",
    "exposures.cigarettes_per_day",
    "exposures.height",
    "exposures.updated_datetime",
    "exposures.weight",
    "exposures.years_smoked"
]

fields = ['case_id'] + demographic_fields + diagnoses_fields + exposures_fields

cases_filters = [
    ('cases.project.program.name', 'in', ['TCGA']),
]

cases_path = f"{volume_path}/cases.tsv"

# Check if file exists
try:
    cases_exists = len(dbutils.fs.ls(cases_path)) > 0
except Exception:
    cases_exists = False

if not cases_exists or FORCE_DOWNLOAD:
    logger.info(f'Downloading cases data to {cases_path}')
    download_table(
        cases_endpt,
        fields,
        cases_path,
        size=100000,
        filters=cases_filters
    )
else:
    logger.info(f'File {cases_path} already exists')

# Read and validate cases data
cases_df = spark.read.csv(cases_path, sep='\t', header=True, inferSchema=True)
case_count = cases_df.count()
logger.info(f"Loaded {case_count} case records")
display(cases_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Download Gene Expression Files

# COMMAND ----------

# MAGIC %md
# MAGIC Download individual gene expression profiles. This will download thousands of files and may take some time, even with concurrent downloads enabled.

# COMMAND ----------

# DBTITLE 1,Download expression files
EXPRESSION_FILES_PATH = f"{volume_path}/expressions"

# Create directory if it doesn't exist
try:
    dbutils.fs.mkdirs(EXPRESSION_FILES_PATH)
    logger.info(f"Expression files directory: {EXPRESSION_FILES_PATH}")
except Exception as e:
    logger.warning(f"Directory may already exist: {str(e)}")

# Get list of UUIDs to download
uuids = [row.file_id for row in files_list_df.select("file_id").collect()]
logger.info(f"Total files to download: {len(uuids)}")

# Check for existing files to skip if not forcing download
if not FORCE_DOWNLOAD:
    try:
        existing_files = {file.name for file in dbutils.fs.ls(EXPRESSION_FILES_PATH)}
        uuids_to_download = [uuid for uuid in uuids if uuid not in existing_files]
        logger.info(f"Found {len(existing_files)} existing files, will download {len(uuids_to_download)} new files")
        uuids = uuids_to_download
    except Exception:
        logger.info("No existing files found, will download all")

# Download expressions with progress tracking
if uuids:
    download_stats = download_expressions(
        EXPRESSION_FILES_PATH,
        uuids,
        n_workers=MAX_WORKERS
    )

    # Display download statistics
    print("\n" + "="*50)
    print("Download Summary:")
    print("="*50)
    print(f"Total files: {download_stats['total']}")
    print(f"Successful: {download_stats['successful']}")
    print(f"Failed: {download_stats['failed']}")
    if download_stats['errors']:
        print(f"\nFirst 10 errors:")
        for error in download_stats['errors'][:10]:
            print(f"  - {error}")
    print("="*50)
else:
    logger.info("All expression files already downloaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Data Download Complete
# MAGIC
# MAGIC The following data has been downloaded to the Unity Catalog volume:
# MAGIC - Expression files metadata
# MAGIC - Clinical cases data
# MAGIC - Gene expression profiles
# MAGIC
# MAGIC Next steps:
# MAGIC - Run the DLT pipeline to create managed tables from the raw files
# MAGIC - Perform quality checks and validation
# MAGIC - Run analysis notebooks

# COMMAND ----------

