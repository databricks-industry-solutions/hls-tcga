"""
Configuration Loader Utility

Provides utilities for reading pipeline configuration from Delta table.
All notebooks and scripts should use this module to access configuration.

Usage:
    from utils.config_loader import get_active_config, get_config_by_id

    # Get active configuration
    config = get_active_config(spark, catalog, schema)

    # Access configuration values
    volume_path = config['volume_path']
    max_workers = config['max_workers']
"""

from typing import Dict, Any, Optional
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


class ConfigLoader:
    """
    Configuration loader for TCGA pipeline.

    Reads configuration from Delta table and provides easy access to config values.
    """

    def __init__(self, spark: SparkSession, catalog: str, schema: str):
        """
        Initialize configuration loader.

        Args:
            spark: SparkSession instance
            catalog: Unity Catalog name
            schema: Schema name where config table is stored
        """
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
        self.config_table = f"{catalog}.{schema}.pipeline_config"
        self._config_cache = None

    def get_active_config(self, use_cache: bool = True) -> Dict[str, Any]:
        """
        Get the currently active configuration.

        Args:
            use_cache: Whether to use cached config (default True)

        Returns:
            Dictionary with configuration values

        Raises:
            RuntimeError: If no active configuration found
        """
        if use_cache and self._config_cache is not None:
            return self._config_cache

        try:
            # Read active configuration
            config_df = self.spark.sql(f"""
                SELECT *
                FROM {self.config_table}
                WHERE is_active = true
                ORDER BY config_timestamp DESC
                LIMIT 1
            """)

            if config_df.count() == 0:
                raise RuntimeError(
                    f"No active configuration found in {self.config_table}. "
                    "Please run 00-load-config.py first."
                )

            # Convert to dictionary
            config_row = config_df.first()
            config_dict = config_row.asDict()

            # Cache the config
            self._config_cache = config_dict

            return config_dict

        except Exception as e:
            raise RuntimeError(
                f"Failed to load configuration from {self.config_table}: {str(e)}"
            )

    def get_config_by_id(self, config_id: str) -> Dict[str, Any]:
        """
        Get configuration by specific ID.

        Args:
            config_id: Configuration UUID

        Returns:
            Dictionary with configuration values

        Raises:
            RuntimeError: If configuration ID not found
        """
        try:
            config_df = self.spark.sql(f"""
                SELECT *
                FROM {self.config_table}
                WHERE config_id = '{config_id}'
                LIMIT 1
            """)

            if config_df.count() == 0:
                raise RuntimeError(
                    f"Configuration ID '{config_id}' not found in {self.config_table}"
                )

            # Convert to dictionary
            config_row = config_df.first()
            return config_row.asDict()

        except Exception as e:
            raise RuntimeError(
                f"Failed to load configuration {config_id}: {str(e)}"
            )

    def list_configs(self, limit: int = 10) -> None:
        """
        Display recent configurations.

        Args:
            limit: Maximum number of configs to display
        """
        configs_df = self.spark.sql(f"""
            SELECT
                config_id,
                config_timestamp,
                is_active,
                catalog,
                schema,
                volume,
                max_workers,
                max_records
            FROM {self.config_table}
            ORDER BY config_timestamp DESC
            LIMIT {limit}
        """)

        configs_df.show(truncate=False)

    def get_config_value(self, key: str, default: Any = None) -> Any:
        """
        Get a specific configuration value by key.

        Args:
            key: Configuration key name
            default: Default value if key not found

        Returns:
            Configuration value or default
        """
        config = self.get_active_config()
        return config.get(key, default)

    def get_lakehouse_config(self) -> Dict[str, str]:
        """Get lakehouse-related configuration."""
        config = self.get_active_config()
        return {
            'catalog': config['catalog'],
            'schema': config['schema'],
            'volume': config['volume'],
            'volume_path': config['volume_path'],
            'database_name': config['database_name']
        }

    def get_api_config(self) -> Dict[str, str]:
        """Get API endpoint configuration."""
        config = self.get_active_config()
        return {
            'cases_endpt': config['cases_endpt'],
            'files_endpt': config['files_endpt'],
            'data_endpt': config['data_endpt']
        }

    def get_pipeline_config(self) -> Dict[str, Any]:
        """Get pipeline execution configuration."""
        config = self.get_active_config()
        return {
            'max_workers': config['max_workers'],
            'max_records': config['max_records'],
            'force_download': config['force_download'],
            'retry_attempts': config['retry_attempts'],
            'timeout_seconds': config['timeout_seconds']
        }

    def get_compute_config(self) -> Dict[str, str]:
        """Get compute resource configuration."""
        config = self.get_active_config()
        return {
            'download_node_type': config.get('download_node_type'),
            'etl_node_type': config.get('etl_node_type'),
            'analysis_node_type': config.get('analysis_node_type')
        }


# Convenience functions for quick access
def get_active_config(
    spark: SparkSession,
    catalog: str,
    schema: str
) -> Dict[str, Any]:
    """
    Quick access to active configuration.

    Args:
        spark: SparkSession instance
        catalog: Unity Catalog name
        schema: Schema name

    Returns:
        Dictionary with configuration values

    Example:
        >>> config = get_active_config(spark, "kermany", "tcga")
        >>> volume_path = config['volume_path']
    """
    loader = ConfigLoader(spark, catalog, schema)
    return loader.get_active_config()


def get_config_by_id(
    spark: SparkSession,
    catalog: str,
    schema: str,
    config_id: str
) -> Dict[str, Any]:
    """
    Quick access to configuration by ID.

    Args:
        spark: SparkSession instance
        catalog: Unity Catalog name
        schema: Schema name
        config_id: Configuration UUID

    Returns:
        Dictionary with configuration values
    """
    loader = ConfigLoader(spark, catalog, schema)
    return loader.get_config_by_id(config_id)


def create_config_loader(
    spark: SparkSession,
    catalog: str = None,
    schema: str = None
) -> ConfigLoader:
    """
    Create ConfigLoader instance.

    If catalog/schema not provided, tries to read from widgets or environment.

    Args:
        spark: SparkSession instance
        catalog: Optional catalog name
        schema: Optional schema name

    Returns:
        ConfigLoader instance
    """
    # Try to get from widgets if not provided
    if catalog is None or schema is None:
        try:
            catalog = catalog or dbutils.widgets.get("catalog")
            schema = schema or dbutils.widgets.get("schema")
        except:
            raise ValueError(
                "catalog and schema must be provided or available as widgets"
            )

    return ConfigLoader(spark, catalog, schema)
