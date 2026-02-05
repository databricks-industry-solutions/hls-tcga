"""
Configuration Manager for TCGA Project

This module provides utilities for loading, validating, and accessing configuration
for the TCGA data pipeline. It supports environment-specific configurations and
integration with Databricks secrets.
"""

import json
import os
from typing import Dict, Any, Optional
from dataclasses import dataclass


@dataclass
class LakehouseConfig:
    """Configuration for Unity Catalog lakehouse resources"""
    catalog: str
    schema: str
    volume: str

    @property
    def volume_path(self) -> str:
        """Get the full volume path"""
        return f'/Volumes/{self.catalog}/{self.schema}/{self.volume}'

    @property
    def database_name(self) -> str:
        """Get the full database name"""
        return f'{self.catalog}.{self.schema}'

    def validate(self) -> None:
        """Validate lakehouse configuration"""
        if not self.catalog or self.catalog == "<CHANGE TO YOUR CATALOG NAME>":
            raise ValueError("Catalog name must be configured in config.json")
        if not self.schema:
            raise ValueError("Schema name must be configured")
        if not self.volume:
            raise ValueError("Volume name must be configured")


@dataclass
class APIConfig:
    """Configuration for GDC API endpoints"""
    cases_endpt: str
    files_endpt: str
    data_endpt: str

    def validate(self) -> None:
        """Validate API configuration"""
        if not all([self.cases_endpt, self.files_endpt, self.data_endpt]):
            raise ValueError("All API endpoints must be configured")
        if not all([e.startswith('http') for e in [self.cases_endpt, self.files_endpt, self.data_endpt]]):
            raise ValueError("All API endpoints must be valid URLs")


@dataclass
class PipelineConfig:
    """Configuration for pipeline execution parameters"""
    max_workers: int = 64
    max_records: int = 20000
    force_download: bool = False
    retry_attempts: int = 3
    timeout_seconds: int = 300

    def validate(self) -> None:
        """Validate pipeline configuration"""
        if self.max_workers < 1 or self.max_workers > 1000:
            raise ValueError("max_workers must be between 1 and 1000")
        if self.max_records < 1:
            raise ValueError("max_records must be positive")
        if self.retry_attempts < 0:
            raise ValueError("retry_attempts must be non-negative")
        if self.timeout_seconds < 1:
            raise ValueError("timeout_seconds must be positive")


class ConfigManager:
    """
    Manages configuration for the TCGA project.

    Supports loading from:
    - JSON configuration files
    - Environment variables
    - Databricks widgets (if in Databricks environment)
    - Databricks secrets (if in Databricks environment)
    """

    def __init__(
        self,
        config_path: Optional[str] = None,
        environment: str = "production"
    ):
        """
        Initialize configuration manager.

        Args:
            config_path: Path to config JSON file. If None, searches default locations.
            environment: Environment name (dev, staging, production)
        """
        self.environment = environment
        self.config_path = config_path or self._find_config_file()
        self._config_data = self._load_config()

        # Initialize configuration objects
        self.lakehouse = self._load_lakehouse_config()
        self.api = self._load_api_config()
        self.pipeline = self._load_pipeline_config()

        # Validate all configurations
        self.validate()

    def _find_config_file(self) -> str:
        """Find config file in default locations"""
        possible_paths = [
            './config.json',
            '../config.json',
            '../../config.json',
            os.path.join(os.path.dirname(__file__), '../config.json'),
            f'./config.{self.environment}.json',
        ]

        for path in possible_paths:
            abs_path = os.path.abspath(path)
            if os.path.exists(abs_path):
                return abs_path

        raise FileNotFoundError(
            f"Config file not found in any of: {possible_paths}"
        )

    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from JSON file"""
        try:
            with open(self.config_path, 'r') as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in config file: {e}")
        except Exception as e:
            raise RuntimeError(f"Failed to load config file: {e}")

    def _load_lakehouse_config(self) -> LakehouseConfig:
        """Load lakehouse configuration"""
        lakehouse_data = self._config_data.get('lakehouse', {})

        # Override with environment variables if present
        catalog = os.getenv('TCGA_CATALOG', lakehouse_data.get('catalog', ''))
        schema = os.getenv('TCGA_SCHEMA', lakehouse_data.get('schema', 'tcga'))
        volume = os.getenv('TCGA_VOLUME', lakehouse_data.get('volume', 'tcga_files'))

        return LakehouseConfig(catalog=catalog, schema=schema, volume=volume)

    def _load_api_config(self) -> APIConfig:
        """Load API configuration"""
        api_data = self._config_data.get('api_paths', {})

        return APIConfig(
            cases_endpt=api_data.get('cases_endpt', ''),
            files_endpt=api_data.get('files_endpt', ''),
            data_endpt=api_data.get('data_endpt', '')
        )

    def _load_pipeline_config(self) -> PipelineConfig:
        """Load pipeline configuration"""
        pipeline_data = self._config_data.get('pipeline', {})

        return PipelineConfig(
            max_workers=int(os.getenv('TCGA_MAX_WORKERS', pipeline_data.get('max_workers', 64))),
            max_records=int(os.getenv('TCGA_MAX_RECORDS', pipeline_data.get('max_records', 20000))),
            force_download=os.getenv('TCGA_FORCE_DOWNLOAD', '').lower() == 'true',
            retry_attempts=int(pipeline_data.get('retry_attempts', 3)),
            timeout_seconds=int(pipeline_data.get('timeout_seconds', 300))
        )

    def validate(self) -> None:
        """Validate all configurations"""
        self.lakehouse.validate()
        self.api.validate()
        self.pipeline.validate()

    def get_databricks_widgets(self, dbutils) -> None:
        """
        Create and populate Databricks widgets with configuration values.

        Args:
            dbutils: Databricks utilities object
        """
        try:
            # Create widgets
            dbutils.widgets.text("catalog", self.lakehouse.catalog, "Catalog")
            dbutils.widgets.text("schema", self.lakehouse.schema, "Schema")
            dbutils.widgets.text("volume", self.lakehouse.volume, "Volume")
            dbutils.widgets.text("max_workers", str(self.pipeline.max_workers), "Max Workers")
            dbutils.widgets.text("max_records", str(self.pipeline.max_records), "Max Records")
            dbutils.widgets.dropdown("force_download", "false", ["true", "false"], "Force Download")

            # Update config from widgets
            self.lakehouse.catalog = dbutils.widgets.get("catalog")
            self.lakehouse.schema = dbutils.widgets.get("schema")
            self.lakehouse.volume = dbutils.widgets.get("volume")
            self.pipeline.max_workers = int(dbutils.widgets.get("max_workers"))
            self.pipeline.max_records = int(dbutils.widgets.get("max_records"))
            self.pipeline.force_download = dbutils.widgets.get("force_download").lower() == "true"

            # Re-validate
            self.validate()

        except Exception as e:
            print(f"Warning: Could not create Databricks widgets: {e}")

    def to_dict(self) -> Dict[str, Any]:
        """Export configuration as dictionary"""
        return {
            'lakehouse': {
                'catalog': self.lakehouse.catalog,
                'schema': self.lakehouse.schema,
                'volume': self.lakehouse.volume,
                'volume_path': self.lakehouse.volume_path,
                'database_name': self.lakehouse.database_name
            },
            'api_paths': {
                'cases_endpt': self.api.cases_endpt,
                'files_endpt': self.api.files_endpt,
                'data_endpt': self.api.data_endpt
            },
            'pipeline': {
                'max_workers': self.pipeline.max_workers,
                'max_records': self.pipeline.max_records,
                'force_download': self.pipeline.force_download,
                'retry_attempts': self.pipeline.retry_attempts,
                'timeout_seconds': self.pipeline.timeout_seconds
            },
            'environment': self.environment
        }

    def __repr__(self) -> str:
        """String representation of configuration"""
        return f"ConfigManager(environment={self.environment}, catalog={self.lakehouse.catalog})"


# Convenience function for quick configuration loading
def load_config(config_path: Optional[str] = None, environment: str = "production") -> ConfigManager:
    """
    Load configuration with validation.

    Args:
        config_path: Optional path to config file
        environment: Environment name (dev, staging, production)

    Returns:
        ConfigManager instance

    Example:
        >>> config = load_config()
        >>> print(config.lakehouse.volume_path)
        /Volumes/my_catalog/tcga/tcga_files
    """
    return ConfigManager(config_path=config_path, environment=environment)
