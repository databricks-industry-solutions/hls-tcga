# TCGA Utilities

This directory contains utility modules for error handling, logging, and common operations.

## Modules

### `error_handling.py`

Comprehensive error handling utilities including:

#### Custom Exceptions
- `TCGAError`: Base exception for the project
- `ConfigurationError`: Configuration validation errors
- `DataDownloadError`: Data download failures
- `DataValidationError`: Data validation failures
- `PipelineError`: Pipeline execution failures
- `DataSizeError`: Data size limit exceeded

#### Retry Decorator

```python
from utils.error_handling import retry_with_backoff
import requests

@retry_with_backoff(
    max_attempts=3,
    initial_delay=1.0,
    exceptions=(requests.RequestException,)
)
def download_data(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.content
```

#### Error Context Manager

```python
from utils.error_handling import ErrorHandler
import logging

logger = logging.getLogger(__name__)

with ErrorHandler(
    operation_name="data_processing",
    logger=logger,
    notify_on_error=True,
    record_to_dlq=True
):
    process_data()  # Any errors will be logged, recorded, and optionally notified
```

#### Dead Letter Queue

```python
from utils.error_handling import DeadLetterQueue

dlq = DeadLetterQueue()

# Get error summary
summary = dlq.get_error_summary()
print(f"Total errors: {summary['total_errors']}")
print(f"Error types: {summary['error_types']}")

# Read errors
errors = dlq.read_errors(limit=10)
for error in errors:
    print(f"Operation: {error['operation']}")
    print(f"Error: {error['error_message']}")

# Clear DLQ
count = dlq.clear_dlq()
print(f"Cleared {count} errors")
```

#### Validation Helpers

```python
from utils.error_handling import (
    validate_dataframe_not_empty,
    validate_required_columns,
    validate_data_size
)

# Validate DataFrame
validate_dataframe_not_empty(df, operation="data_load")

# Validate required columns
validate_required_columns(
    df,
    required_columns=['file_id', 'gene_id', 'fpkm_unstranded'],
    operation="expression_processing"
)

# Validate data size before pandas conversion
validate_data_size(
    n_rows=10000,
    n_cols=1000,
    max_size_mb=2000,
    operation="pivot_operation"
)
```

#### Execution Time Logger

```python
from utils.error_handling import log_execution_time

@log_execution_time
def expensive_operation():
    # Your code here
    pass
```

### `logging_utils.py`

Structured logging utilities with consistent formatting:

#### Basic Logger Setup

```python
from utils.logging_utils import setup_logger

# Console logger with colors
logger = setup_logger(__name__, level="INFO", use_colors=True)

# JSON logger for log aggregation
logger = setup_logger(
    __name__,
    level="DEBUG",
    use_json_format=True,
    log_to_file=True,
    log_file_path="/tmp/my_app.log"
)

# Use the logger
logger.info("Processing started")
logger.warning("Memory usage high")
logger.error("Operation failed", extra={'extra_fields': {'error_code': 'E001'}})
```

#### Databricks Logger

```python
from utils.logging_utils import get_databricks_logger

logger = get_databricks_logger(__name__)
logger.info("Databricks job started")
```

#### Progress Logger

```python
from utils.logging_utils import ProgressLogger

logger = setup_logger(__name__)
progress = ProgressLogger(
    logger,
    total=10000,
    operation="Downloading files",
    log_interval=100  # Log every 100 items
)

for item in items:
    process(item)
    progress.update(1)

progress.finish()
```

Output:
```
[INFO] Downloading files: 100/10000 (1.0%) - Rate: 50.0 items/s - ETA: 3.3m
[INFO] Downloading files: 200/10000 (2.0%) - Rate: 52.3 items/s - ETA: 3.1m
...
[INFO] Downloading files complete: 10000 items in 3.2m (avg rate: 52.1 items/s)
```

#### Contextual Logger

```python
from utils.logging_utils import ContextualLogger

base_logger = setup_logger(__name__)
logger = ContextualLogger(
    base_logger,
    context={"job_id": "job_123", "run_id": "run_456"}
)

logger.info("Processing started")
# Output: [INFO] [job_id=job_123 | run_id=run_456] Processing started

logger.update_context(batch_id="batch_789")
logger.info("Batch processing")
# Output: [INFO] [job_id=job_123 | run_id=run_456 | batch_id=batch_789] Batch processing
```

#### Utility Functions

```python
from utils.logging_utils import log_dataframe_info, log_dict

# Log DataFrame info
log_dataframe_info(logger, df, name="Expression Profiles")
# Output: [INFO] Expression Profiles info: 10,000 rows, 5 columns. Columns: file_id, gene_id, ...

# Log dictionary
config = {"catalog": "main", "schema": "tcga", "max_workers": 64}
log_dict(logger, config, name="Configuration", level="DEBUG")
# Output:
# [DEBUG] Configuration:
# [DEBUG]   catalog: main
# [DEBUG]   schema: tcga
# [DEBUG]   max_workers: 64
```

## Integration Examples

### Complete Example: Data Download with Error Handling

```python
from utils.logging_utils import setup_logger, ProgressLogger
from utils.error_handling import (
    retry_with_backoff,
    ErrorHandler,
    DataDownloadError
)
import requests

# Setup logger
logger = setup_logger(__name__, level="INFO")

# Download function with retry
@retry_with_backoff(
    max_attempts=3,
    exceptions=(requests.RequestException,),
    logger=logger
)
def download_file(url):
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.content

# Main download operation with error handling
def download_all_files(urls):
    with ErrorHandler(
        operation_name="bulk_file_download",
        logger=logger,
        record_to_dlq=True
    ):
        progress = ProgressLogger(logger, total=len(urls), operation="Downloading")

        results = []
        for url in urls:
            try:
                content = download_file(url)
                results.append((url, content))
                progress.update(1)
            except Exception as e:
                logger.error(f"Failed to download {url}: {str(e)}")
                # Continue with other files

        progress.finish()
        return results

# Run download
urls = ["https://api.example.com/file1", "https://api.example.com/file2"]
results = download_all_files(urls)
```

### Example: DLT Pipeline with Validation

```python
import dlt
from pyspark.sql.functions import col
from utils.logging_utils import get_databricks_logger
from utils.error_handling import (
    validate_dataframe_not_empty,
    validate_required_columns,
    ErrorHandler
)

logger = get_databricks_logger(__name__)

@dlt.table
def validated_table():
    with ErrorHandler(
        operation_name="table_creation",
        logger=logger,
        record_to_dlq=True
    ):
        # Read source data
        df = spark.read.table("source_table")

        # Validate
        validate_dataframe_not_empty(df, operation="source_table_read")
        validate_required_columns(
            df,
            required_columns=['id', 'value', 'timestamp'],
            operation="source_table_validation"
        )

        logger.info(f"Processing {df.count():,} records")

        # Transform
        result = df.filter(col('value').isNotNull())

        logger.info(f"Returning {result.count():,} records after filtering")

        return result
```

## Best Practices

1. **Always use structured logging**: Makes log parsing and analysis easier
2. **Include context in logs**: Job IDs, run IDs, operation names
3. **Log at appropriate levels**:
   - DEBUG: Detailed diagnostic information
   - INFO: General informational messages
   - WARNING: Warning messages for unexpected but handled situations
   - ERROR: Error messages for failures
   - CRITICAL: Critical issues requiring immediate attention
4. **Use retry decorator for transient failures**: Network requests, external APIs
5. **Use error context manager for complex operations**: Ensures errors are logged and tracked
6. **Monitor the DLQ**: Regularly review and address recurring errors
7. **Log execution times**: Helps identify performance bottlenecks
8. **Use progress loggers for long operations**: Provides visibility into progress

## Integration with Databricks

### Job Notifications

In Databricks jobs, errors are automatically captured and can trigger email notifications. To integrate with custom error handling:

```python
# In a Databricks notebook
from utils.error_handling import ErrorHandler

def send_databricks_notification(error_details):
    # Fail the notebook to trigger job alerts
    dbutils.notebook.exit(json.dumps({
        "status": "failed",
        "error": error_details['error_message']
    }))

with ErrorHandler(
    operation_name="critical_operation",
    notify_on_error=True,
    notification_handler=send_databricks_notification
):
    run_critical_operation()
```

### MLflow Integration

Log errors to MLflow for tracking:

```python
import mlflow
from utils.error_handling import ErrorHandler

def log_to_mlflow(error_details):
    mlflow.log_param("error_occurred", True)
    mlflow.log_param("error_type", error_details['error_type'])
    mlflow.log_text(error_details['traceback'], "error_traceback.txt")

with ErrorHandler(
    operation_name="ml_training",
    notification_handler=log_to_mlflow
):
    train_model()
```

## Testing

To test error handling:

```python
# Test retry decorator
@retry_with_backoff(max_attempts=3, initial_delay=0.1)
def flaky_function():
    import random
    if random.random() < 0.7:
        raise Exception("Random failure")
    return "Success"

# Test error handler
with ErrorHandler("test_operation", record_to_dlq=True, raise_on_error=False):
    raise ValueError("Test error")

# Check DLQ
dlq = DeadLetterQueue()
print(dlq.get_error_summary())
```
