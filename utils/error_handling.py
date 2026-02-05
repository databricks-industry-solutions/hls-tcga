"""
Error Handling Utilities for TCGA Project

This module provides comprehensive error handling utilities including:
- Custom exception classes
- Retry decorators with exponential backoff
- Error notification handlers
- Context managers for error handling
- Dead letter queue for failed operations
"""

import functools
import time
import logging
import traceback
from typing import Callable, Optional, Type, Tuple, Any, Dict, List
from datetime import datetime
import json


# ============================================================================
# Custom Exceptions
# ============================================================================

class TCGAError(Exception):
    """Base exception for TCGA project"""
    pass


class ConfigurationError(TCGAError):
    """Raised when configuration is invalid or missing"""
    pass


class DataDownloadError(TCGAError):
    """Raised when data download fails"""
    pass


class DataValidationError(TCGAError):
    """Raised when data validation fails"""
    pass


class PipelineError(TCGAError):
    """Raised when pipeline execution fails"""
    pass


class DataSizeError(TCGAError):
    """Raised when data size exceeds limits"""
    pass


# ============================================================================
# Retry Decorator
# ============================================================================

def retry_with_backoff(
    max_attempts: int = 3,
    initial_delay: float = 1.0,
    backoff_factor: float = 2.0,
    max_delay: float = 60.0,
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
    logger: Optional[logging.Logger] = None
) -> Callable:
    """
    Decorator that retries a function with exponential backoff.

    Args:
        max_attempts: Maximum number of retry attempts
        initial_delay: Initial delay in seconds before first retry
        backoff_factor: Factor by which delay increases each retry
        max_delay: Maximum delay between retries in seconds
        exceptions: Tuple of exception types to catch and retry
        logger: Optional logger for logging retry attempts

    Returns:
        Decorated function with retry logic

    Example:
        @retry_with_backoff(max_attempts=3, exceptions=(requests.RequestException,))
        def download_data(url):
            response = requests.get(url)
            response.raise_for_status()
            return response.content
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            delay = initial_delay
            last_exception = None

            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt == max_attempts:
                        logger.error(
                            f"Function {func.__name__} failed after {max_attempts} attempts. "
                            f"Last error: {str(e)}"
                        )
                        raise

                    logger.warning(
                        f"Function {func.__name__} failed (attempt {attempt}/{max_attempts}). "
                        f"Retrying in {delay:.2f}s. Error: {str(e)}"
                    )
                    time.sleep(delay)
                    delay = min(delay * backoff_factor, max_delay)

            # This should never be reached, but just in case
            raise last_exception

        return wrapper
    return decorator


# ============================================================================
# Error Context Manager
# ============================================================================

class ErrorHandler:
    """
    Context manager for comprehensive error handling.

    Features:
    - Logs errors with full context
    - Optionally sends notifications
    - Records errors to dead letter queue
    - Provides error recovery options

    Example:
        with ErrorHandler(operation_name="data_download", logger=logger):
            download_data_from_api()
    """

    def __init__(
        self,
        operation_name: str,
        logger: Optional[logging.Logger] = None,
        notify_on_error: bool = False,
        notification_handler: Optional[Callable] = None,
        record_to_dlq: bool = True,
        dlq_path: Optional[str] = None,
        raise_on_error: bool = True
    ):
        """
        Initialize error handler context manager.

        Args:
            operation_name: Name of the operation being performed
            logger: Logger instance for logging errors
            notify_on_error: Whether to send notifications on error
            notification_handler: Function to call for notifications
            record_to_dlq: Whether to record errors to dead letter queue
            dlq_path: Path to dead letter queue (defaults to /tmp/tcga_dlq.jsonl)
            raise_on_error: Whether to re-raise the exception after handling
        """
        self.operation_name = operation_name
        self.logger = logger or logging.getLogger(__name__)
        self.notify_on_error = notify_on_error
        self.notification_handler = notification_handler
        self.record_to_dlq = record_to_dlq
        self.dlq_path = dlq_path or "/tmp/tcga_dlq.jsonl"
        self.raise_on_error = raise_on_error
        self.start_time = None

    def __enter__(self):
        self.start_time = datetime.now()
        self.logger.info(f"Starting operation: {self.operation_name}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            # Success
            duration = (datetime.now() - self.start_time).total_seconds()
            self.logger.info(
                f"Operation completed successfully: {self.operation_name} "
                f"(duration: {duration:.2f}s)"
            )
            return True

        # Error occurred
        duration = (datetime.now() - self.start_time).total_seconds()
        error_details = {
            "operation": self.operation_name,
            "timestamp": self.start_time.isoformat(),
            "duration_seconds": duration,
            "error_type": exc_type.__name__,
            "error_message": str(exc_val),
            "traceback": traceback.format_exc()
        }

        # Log error
        self.logger.error(
            f"Operation failed: {self.operation_name} "
            f"(duration: {duration:.2f}s)\n"
            f"Error: {exc_type.__name__}: {str(exc_val)}\n"
            f"Traceback:\n{traceback.format_exc()}"
        )

        # Record to dead letter queue
        if self.record_to_dlq:
            try:
                self._record_to_dlq(error_details)
            except Exception as dlq_error:
                self.logger.error(f"Failed to record to DLQ: {str(dlq_error)}")

        # Send notification
        if self.notify_on_error and self.notification_handler:
            try:
                self.notification_handler(error_details)
            except Exception as notify_error:
                self.logger.error(f"Failed to send notification: {str(notify_error)}")

        # Return False to re-raise if configured
        return not self.raise_on_error

    def _record_to_dlq(self, error_details: Dict[str, Any]) -> None:
        """Record error to dead letter queue"""
        try:
            with open(self.dlq_path, 'a') as f:
                f.write(json.dumps(error_details) + '\n')
            self.logger.info(f"Error recorded to DLQ: {self.dlq_path}")
        except Exception as e:
            self.logger.error(f"Failed to write to DLQ: {str(e)}")


# ============================================================================
# Dead Letter Queue Manager
# ============================================================================

class DeadLetterQueue:
    """
    Manager for dead letter queue operations.

    Provides methods to read, analyze, and replay failed operations.
    """

    def __init__(self, dlq_path: str = "/tmp/tcga_dlq.jsonl"):
        """
        Initialize DLQ manager.

        Args:
            dlq_path: Path to dead letter queue file
        """
        self.dlq_path = dlq_path
        self.logger = logging.getLogger(__name__)

    def read_errors(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Read errors from DLQ.

        Args:
            limit: Maximum number of errors to read

        Returns:
            List of error dictionaries
        """
        errors = []
        try:
            with open(self.dlq_path, 'r') as f:
                for i, line in enumerate(f):
                    if limit and i >= limit:
                        break
                    try:
                        errors.append(json.loads(line))
                    except json.JSONDecodeError:
                        self.logger.warning(f"Failed to parse DLQ line {i+1}")
        except FileNotFoundError:
            self.logger.info(f"DLQ file not found: {self.dlq_path}")
        return errors

    def get_error_summary(self) -> Dict[str, Any]:
        """
        Get summary statistics of errors in DLQ.

        Returns:
            Dictionary with error statistics
        """
        errors = self.read_errors()
        if not errors:
            return {"total_errors": 0}

        error_types = {}
        operations = {}
        for error in errors:
            error_type = error.get('error_type', 'Unknown')
            operation = error.get('operation', 'Unknown')
            error_types[error_type] = error_types.get(error_type, 0) + 1
            operations[operation] = operations.get(operation, 0) + 1

        return {
            "total_errors": len(errors),
            "error_types": error_types,
            "operations": operations,
            "oldest_error": errors[0].get('timestamp') if errors else None,
            "newest_error": errors[-1].get('timestamp') if errors else None
        }

    def clear_dlq(self) -> int:
        """
        Clear all errors from DLQ.

        Returns:
            Number of errors cleared
        """
        errors = self.read_errors()
        count = len(errors)
        try:
            with open(self.dlq_path, 'w') as f:
                f.write('')
            self.logger.info(f"Cleared {count} errors from DLQ")
        except Exception as e:
            self.logger.error(f"Failed to clear DLQ: {str(e)}")
            raise
        return count


# ============================================================================
# Notification Handlers
# ============================================================================

def email_notification_handler(error_details: Dict[str, Any]) -> None:
    """
    Send email notification for errors (placeholder).

    In production, integrate with your email service or Databricks alerting.

    Args:
        error_details: Dictionary containing error information
    """
    # Placeholder for email notification
    # In Databricks, you can use:
    # - dbutils.notebook.exit() to fail the notebook and trigger job alerts
    # - Custom integration with email services
    # - Webhook to alerting systems
    print(f"EMAIL NOTIFICATION: {error_details['operation']} failed")
    print(f"Error: {error_details['error_type']}: {error_details['error_message']}")


def slack_notification_handler(
    error_details: Dict[str, Any],
    webhook_url: Optional[str] = None
) -> None:
    """
    Send Slack notification for errors (placeholder).

    In production, integrate with Slack webhooks.

    Args:
        error_details: Dictionary containing error information
        webhook_url: Slack webhook URL
    """
    # Placeholder for Slack notification
    # In production, use requests to post to Slack webhook:
    # import requests
    # requests.post(webhook_url, json={"text": message})
    print(f"SLACK NOTIFICATION: {error_details['operation']} failed")
    print(f"Error: {error_details['error_type']}: {error_details['error_message']}")


# ============================================================================
# Validation Helpers
# ============================================================================

def validate_dataframe_not_empty(df, operation: str = "operation") -> None:
    """
    Validate that a Spark DataFrame is not empty.

    Args:
        df: Spark DataFrame to validate
        operation: Name of operation (for error message)

    Raises:
        DataValidationError: If DataFrame is empty
    """
    if df.count() == 0:
        raise DataValidationError(f"{operation}: DataFrame is empty")


def validate_required_columns(df, required_columns: List[str], operation: str = "operation") -> None:
    """
    Validate that a DataFrame contains required columns.

    Args:
        df: DataFrame to validate (Spark or pandas)
        required_columns: List of required column names
        operation: Name of operation (for error message)

    Raises:
        DataValidationError: If required columns are missing
    """
    actual_columns = set(df.columns)
    missing_columns = set(required_columns) - actual_columns

    if missing_columns:
        raise DataValidationError(
            f"{operation}: Missing required columns: {', '.join(missing_columns)}"
        )


def validate_data_size(
    n_rows: int,
    n_cols: int,
    max_size_mb: float = 2000,
    operation: str = "operation"
) -> None:
    """
    Validate that data size is within acceptable limits.

    Args:
        n_rows: Number of rows
        n_cols: Number of columns
        max_size_mb: Maximum size in MB
        operation: Name of operation (for error message)

    Raises:
        DataSizeError: If data size exceeds limit
    """
    estimated_size_mb = (n_rows * n_cols * 8) / (1024 * 1024)
    if estimated_size_mb > max_size_mb:
        raise DataSizeError(
            f"{operation}: Data size ({estimated_size_mb:.2f} MB) exceeds "
            f"maximum ({max_size_mb} MB). Consider sampling or using distributed computation."
        )


# ============================================================================
# Utility Functions
# ============================================================================

def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """
    Safely divide two numbers, returning default if denominator is zero.

    Args:
        numerator: Numerator value
        denominator: Denominator value
        default: Default value to return if denominator is zero

    Returns:
        Result of division or default value
    """
    try:
        return numerator / denominator if denominator != 0 else default
    except (TypeError, ValueError):
        return default


def log_execution_time(func: Callable) -> Callable:
    """
    Decorator to log execution time of a function.

    Args:
        func: Function to decorate

    Returns:
        Decorated function that logs execution time
    """
    logger = logging.getLogger(func.__module__)

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        logger.info(f"Starting {func.__name__}")
        try:
            result = func(*args, **kwargs)
            duration = time.time() - start_time
            logger.info(f"Completed {func.__name__} in {duration:.2f}s")
            return result
        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"Failed {func.__name__} after {duration:.2f}s: {str(e)}")
            raise

    return wrapper
