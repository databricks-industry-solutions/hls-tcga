"""
Logging Utilities for TCGA Project

This module provides structured logging utilities with consistent formatting,
log levels, and integration with Databricks.
"""

import logging
import sys
from typing import Optional
from datetime import datetime
import json


# ============================================================================
# Custom Log Formatter
# ============================================================================

class StructuredFormatter(logging.Formatter):
    """
    Structured JSON formatter for logs.

    Outputs logs in JSON format for easy parsing and analysis.
    """

    def format(self, record: logging.LogRecord) -> str:
        """
        Format log record as JSON.

        Args:
            record: Log record to format

        Returns:
            JSON-formatted log string
        """
        log_data = {
            "timestamp": datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno
        }

        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        # Add extra fields if present
        if hasattr(record, 'extra_fields'):
            log_data.update(record.extra_fields)

        return json.dumps(log_data)


class ColoredFormatter(logging.Formatter):
    """
    Colored formatter for console output.

    Uses ANSI color codes for better readability in terminals.
    """

    # ANSI color codes
    COLORS = {
        'DEBUG': '\033[36m',      # Cyan
        'INFO': '\033[32m',       # Green
        'WARNING': '\033[33m',    # Yellow
        'ERROR': '\033[31m',      # Red
        'CRITICAL': '\033[35m',   # Magenta
        'RESET': '\033[0m'        # Reset
    }

    def format(self, record: logging.LogRecord) -> str:
        """
        Format log record with colors.

        Args:
            record: Log record to format

        Returns:
            Colored log string
        """
        color = self.COLORS.get(record.levelname, self.COLORS['RESET'])
        reset = self.COLORS['RESET']

        # Format: [LEVEL] timestamp - module.function:line - message
        log_format = (
            f"{color}[{record.levelname}]{reset} "
            f"{datetime.fromtimestamp(record.created).strftime('%Y-%m-%d %H:%M:%S')} - "
            f"{record.module}.{record.funcName}:{record.lineno} - "
            f"{record.getMessage()}"
        )

        if record.exc_info:
            log_format += f"\n{self.formatException(record.exc_info)}"

        return log_format


# ============================================================================
# Logger Setup
# ============================================================================

def setup_logger(
    name: str,
    level: str = "INFO",
    log_to_file: bool = False,
    log_file_path: Optional[str] = None,
    use_json_format: bool = False,
    use_colors: bool = True
) -> logging.Logger:
    """
    Set up a logger with consistent configuration.

    Args:
        name: Logger name (typically __name__ from calling module)
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_to_file: Whether to log to a file
        log_file_path: Path to log file (if log_to_file is True)
        use_json_format: Whether to use JSON format (useful for log aggregation)
        use_colors: Whether to use colored output for console (ignored if JSON)

    Returns:
        Configured logger instance

    Example:
        logger = setup_logger(__name__, level="DEBUG")
        logger.info("Starting data download")
        logger.error("Download failed", extra={'extra_fields': {'url': url}})
    """
    logger = logging.getLogger(name)

    # Avoid adding handlers multiple times
    if logger.handlers:
        return logger

    logger.setLevel(getattr(logging, level.upper()))

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, level.upper()))

    if use_json_format:
        console_formatter = StructuredFormatter()
    elif use_colors:
        console_formatter = ColoredFormatter()
    else:
        console_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    # File handler (optional)
    if log_to_file:
        file_path = log_file_path or f"/tmp/tcga_{name}_{datetime.now().strftime('%Y%m%d')}.log"
        file_handler = logging.FileHandler(file_path)
        file_handler.setLevel(getattr(logging, level.upper()))

        # Always use structured format for files
        file_formatter = StructuredFormatter()
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

        logger.info(f"Logging to file: {file_path}")

    return logger


def get_databricks_logger(name: str, level: str = "INFO") -> logging.Logger:
    """
    Get a logger configured for Databricks environment.

    Args:
        name: Logger name
        level: Logging level

    Returns:
        Configured logger for Databricks

    Example:
        logger = get_databricks_logger(__name__)
        logger.info("Processing data")
    """
    # In Databricks, logs are automatically captured
    # Use simpler formatting since Databricks adds its own metadata
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(getattr(logging, level.upper()))

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(getattr(logging, level.upper()))

    # Simple format for Databricks
    formatter = logging.Formatter(
        '[%(levelname)s] %(name)s - %(message)s'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


# ============================================================================
# Progress Logger
# ============================================================================

class ProgressLogger:
    """
    Logger for tracking progress of long-running operations.

    Provides methods to log progress at regular intervals without
    overwhelming the logs.
    """

    def __init__(
        self,
        logger: logging.Logger,
        total: int,
        operation: str = "Processing",
        log_interval: int = 100
    ):
        """
        Initialize progress logger.

        Args:
            logger: Logger instance to use
            total: Total number of items to process
            operation: Description of the operation
            log_interval: Log progress every N items
        """
        self.logger = logger
        self.total = total
        self.operation = operation
        self.log_interval = log_interval
        self.processed = 0
        self.start_time = datetime.now()

    def update(self, n: int = 1) -> None:
        """
        Update progress counter.

        Args:
            n: Number of items processed since last update
        """
        self.processed += n

        # Log at intervals
        if self.processed % self.log_interval == 0 or self.processed == self.total:
            self._log_progress()

    def _log_progress(self) -> None:
        """Log current progress"""
        elapsed = (datetime.now() - self.start_time).total_seconds()
        rate = self.processed / elapsed if elapsed > 0 else 0
        percent = (self.processed / self.total * 100) if self.total > 0 else 0

        eta_seconds = (self.total - self.processed) / rate if rate > 0 else 0
        eta_str = self._format_seconds(eta_seconds)

        self.logger.info(
            f"{self.operation}: {self.processed}/{self.total} "
            f"({percent:.1f}%) - "
            f"Rate: {rate:.1f} items/s - "
            f"ETA: {eta_str}"
        )

    @staticmethod
    def _format_seconds(seconds: float) -> str:
        """Format seconds as human-readable string"""
        if seconds < 60:
            return f"{seconds:.0f}s"
        elif seconds < 3600:
            return f"{seconds/60:.1f}m"
        else:
            return f"{seconds/3600:.1f}h"

    def finish(self) -> None:
        """Log completion message"""
        elapsed = (datetime.now() - self.start_time).total_seconds()
        rate = self.processed / elapsed if elapsed > 0 else 0

        self.logger.info(
            f"{self.operation} complete: {self.processed} items in "
            f"{self._format_seconds(elapsed)} "
            f"(avg rate: {rate:.1f} items/s)"
        )


# ============================================================================
# Contextual Logger
# ============================================================================

class ContextualLogger:
    """
    Logger that automatically includes contextual information in all log messages.

    Useful for adding operation-specific context (e.g., job_id, run_id) to all logs.
    """

    def __init__(self, logger: logging.Logger, context: dict):
        """
        Initialize contextual logger.

        Args:
            logger: Base logger instance
            context: Dictionary of contextual information to include in all logs
        """
        self.logger = logger
        self.context = context

    def _add_context(self, message: str) -> str:
        """Add context to message"""
        context_str = " | ".join(f"{k}={v}" for k, v in self.context.items())
        return f"[{context_str}] {message}"

    def debug(self, message: str, **kwargs) -> None:
        """Log debug message with context"""
        self.logger.debug(self._add_context(message), **kwargs)

    def info(self, message: str, **kwargs) -> None:
        """Log info message with context"""
        self.logger.info(self._add_context(message), **kwargs)

    def warning(self, message: str, **kwargs) -> None:
        """Log warning message with context"""
        self.logger.warning(self._add_context(message), **kwargs)

    def error(self, message: str, **kwargs) -> None:
        """Log error message with context"""
        self.logger.error(self._add_context(message), **kwargs)

    def critical(self, message: str, **kwargs) -> None:
        """Log critical message with context"""
        self.logger.critical(self._add_context(message), **kwargs)

    def update_context(self, **kwargs) -> None:
        """Update context dictionary"""
        self.context.update(kwargs)


# ============================================================================
# Utility Functions
# ============================================================================

def log_dataframe_info(logger: logging.Logger, df, name: str = "DataFrame") -> None:
    """
    Log information about a Spark DataFrame.

    Args:
        logger: Logger instance
        df: Spark DataFrame
        name: Name of the dataframe (for logging)
    """
    try:
        row_count = df.count()
        col_count = len(df.columns)
        logger.info(
            f"{name} info: {row_count:,} rows, {col_count} columns. "
            f"Columns: {', '.join(df.columns)}"
        )
    except Exception as e:
        logger.warning(f"Failed to log DataFrame info for {name}: {str(e)}")


def log_dict(logger: logging.Logger, data: dict, name: str = "Data", level: str = "INFO") -> None:
    """
    Log a dictionary in a formatted way.

    Args:
        logger: Logger instance
        data: Dictionary to log
        name: Name/description of the data
        level: Log level (INFO, DEBUG, etc.)
    """
    log_func = getattr(logger, level.lower())
    log_func(f"{name}:")
    for key, value in data.items():
        log_func(f"  {key}: {value}")


# ============================================================================
# Example Usage
# ============================================================================

if __name__ == "__main__":
    # Example: Basic logger
    logger = setup_logger("tcga.example", level="DEBUG", use_colors=True)
    logger.debug("This is a debug message")
    logger.info("This is an info message")
    logger.warning("This is a warning message")
    logger.error("This is an error message")

    # Example: Progress logger
    progress = ProgressLogger(logger, total=1000, operation="Downloading files")
    for i in range(1000):
        progress.update(1)
    progress.finish()

    # Example: Contextual logger
    context_logger = ContextualLogger(logger, {"job_id": "123", "run_id": "456"})
    context_logger.info("Processing started")
    context_logger.update_context(batch_id="789")
    context_logger.info("Batch processing")
