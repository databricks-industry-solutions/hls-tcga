#!/usr/bin/env python3
"""
TCGA Pipeline Deployment Script

This script handles the complete deployment workflow for the TCGA data pipeline:
1. Collects configuration parameters (interactively or from config.json)
2. Creates/updates config.json
3. Validates Databricks workspace connectivity
4. Deploys the pipeline using Databricks Asset Bundles
5. Optionally runs the deployed workflow

Usage:
    python deploy.py                    # Interactive setup and deploy
    python deploy.py --run              # Deploy and run immediately
    python deploy.py --config-only      # Only create config.json
    python deploy.py --deploy-only      # Deploy without running
"""

import json
import os
import sys
import subprocess
import argparse
from pathlib import Path
from typing import Dict, Any, Optional


class Colors:
    """ANSI color codes for terminal output"""
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'


def print_header(text: str):
    """Print a formatted header"""
    print(f"\n{Colors.BOLD}{Colors.HEADER}{'='*70}{Colors.ENDC}")
    print(f"{Colors.BOLD}{Colors.HEADER}{text:^70}{Colors.ENDC}")
    print(f"{Colors.BOLD}{Colors.HEADER}{'='*70}{Colors.ENDC}\n")


def print_success(text: str):
    """Print success message"""
    print(f"{Colors.GREEN}✓ {text}{Colors.ENDC}")


def print_error(text: str):
    """Print error message"""
    print(f"{Colors.RED}✗ {text}{Colors.ENDC}")


def print_info(text: str):
    """Print info message"""
    print(f"{Colors.CYAN}ℹ {text}{Colors.ENDC}")


def print_warning(text: str):
    """Print warning message"""
    print(f"{Colors.YELLOW}⚠ {text}{Colors.ENDC}")


def load_existing_config(config_path: Path) -> Optional[Dict[str, Any]]:
    """Load existing config.json if it exists"""
    if config_path.exists():
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            print_error(f"Invalid JSON in {config_path}: {e}")
            return None
    return None


def prompt_with_default(prompt: str, default: str, required: bool = True) -> str:
    """Prompt user for input with a default value"""
    default_display = f" [{Colors.CYAN}{default}{Colors.ENDC}]" if default else ""
    while True:
        response = input(f"{prompt}{default_display}: ").strip()
        if response:
            return response
        if default:
            return default
        if not required:
            return ""
        print_warning("This field is required. Please provide a value.")


def prompt_yes_no(prompt: str, default: bool = True) -> bool:
    """Prompt user for yes/no answer"""
    default_str = "Y/n" if default else "y/N"
    response = input(f"{prompt} [{default_str}]: ").strip().lower()
    if not response:
        return default
    return response in ['y', 'yes']


def get_databricks_profile() -> Optional[str]:
    """Get available Databricks profiles"""
    try:
        result = subprocess.run(
            ['databricks', 'auth', 'profiles'],
            capture_output=True,
            text=True,
            check=True
        )

        # Parse profiles
        profiles = []
        for line in result.stdout.strip().split('\n')[1:]:  # Skip header
            parts = line.split()
            if len(parts) >= 3:
                name, host, valid = parts[0], parts[1], parts[2]
                if valid == "YES":
                    profiles.append((name, host))

        return profiles
    except subprocess.CalledProcessError:
        return None


def select_profile(profiles: list) -> str:
    """Let user select a Databricks profile"""
    print_info("Available Databricks profiles:")
    for i, (name, host) in enumerate(profiles, 1):
        cloud_type = "AWS" if "cloud.databricks.com" in host else "Azure" if "azuredatabricks.net" in host else "Unknown"
        print(f"  {i}. {Colors.BOLD}{name}{Colors.ENDC} - {host} ({cloud_type})")

    while True:
        try:
            choice = input(f"\nSelect profile [1-{len(profiles)}]: ").strip()
            idx = int(choice) - 1
            if 0 <= idx < len(profiles):
                return profiles[idx][0]
            print_warning(f"Please enter a number between 1 and {len(profiles)}")
        except ValueError:
            print_warning("Please enter a valid number")


def detect_cloud_provider(profile: str) -> str:
    """Detect cloud provider from profile"""
    try:
        result = subprocess.run(
            ['databricks', 'current-user', 'me', '--profile', profile],
            capture_output=True,
            text=True,
            check=True
        )
        # Simple heuristic: check workspace URL
        if 'azuredatabricks.net' in result.stdout:
            return 'azure'
        else:
            return 'aws'
    except:
        return 'aws'  # Default to AWS


def get_current_username(profile: str) -> Optional[str]:
    """Get current Databricks username from workspace"""
    try:
        result = subprocess.run(
            ['databricks', 'current-user', 'me', '--profile', profile, '--output', 'json'],
            capture_output=True,
            text=True,
            check=True
        )
        import json
        user_info = json.loads(result.stdout)
        # Try to get username from different possible fields
        username = user_info.get('userName')
        if not username:
            # Try emails field
            emails = user_info.get('emails', [])
            if emails and len(emails) > 0:
                username = emails[0].get('value')
        return username
    except Exception as e:
        print_warning(f"Could not detect username: {e}")
        return None


def get_default_instance_types(cloud: str) -> Dict[str, str]:
    """Get default instance types based on cloud provider"""
    if cloud == 'azure':
        return {
            'download_node_type': 'Standard_E64s_v3',  # 64 vCPUs, 432 GB RAM
            'etl_node_type': 'Standard_E8s_v3',        # 8 vCPUs, 64 GB RAM
            'analysis_node_type': 'Standard_E4s_v3'    # 4 vCPUs, 32 GB RAM
        }
    else:  # AWS
        return {
            'download_node_type': 'r5d.16xlarge',  # 64 vCPUs, 512 GB RAM
            'etl_node_type': 'r5d.2xlarge',        # 8 vCPUs, 64 GB RAM
            'analysis_node_type': 'r5d.xlarge'     # 4 vCPUs, 32 GB RAM
        }


def collect_configuration(existing_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Collect configuration parameters from user"""

    print_header("TCGA Pipeline Configuration")

    # Get or select Databricks profile
    profiles = get_databricks_profile()
    if not profiles:
        print_error("No valid Databricks profiles found. Please configure Databricks CLI first.")
        sys.exit(1)

    if len(profiles) == 1:
        profile = profiles[0][0]
        print_info(f"Using Databricks profile: {Colors.BOLD}{profile}{Colors.ENDC}")
    else:
        profile = select_profile(profiles)

    # Detect cloud provider
    cloud = detect_cloud_provider(profile)
    cloud_display = "AWS" if cloud == 'aws' else "Azure"
    print_success(f"Detected cloud provider: {cloud_display}")

    # Get defaults
    defaults = existing_config or {}
    lakehouse = defaults.get('lakehouse', {})
    pipeline = defaults.get('pipeline', {})
    compute = defaults.get('compute', get_default_instance_types(cloud))

    print("\n" + Colors.BOLD + "Unity Catalog Configuration" + Colors.ENDC)
    print_info("Specify the Unity Catalog resources where data will be stored")

    catalog = prompt_with_default(
        "  Catalog name",
        lakehouse.get('catalog', 'kermany')
    )

    schema = prompt_with_default(
        "  Schema name",
        lakehouse.get('schema', 'tcga')
    )

    volume = prompt_with_default(
        "  Volume name",
        lakehouse.get('volume', 'tcga_files')
    )

    print("\n" + Colors.BOLD + "Pipeline Configuration" + Colors.ENDC)
    print_info("Configure pipeline execution parameters")

    max_workers = prompt_with_default(
        "  Max concurrent download workers",
        str(pipeline.get('max_workers', 64))
    )

    max_records = prompt_with_default(
        "  Max records to download",
        str(pipeline.get('max_records', 20000))
    )

    print("\n" + Colors.BOLD + "Compute Configuration" + Colors.ENDC)
    print_info(f"Configure instance types for {cloud_display}")

    use_defaults = prompt_yes_no(
        f"  Use default {cloud_display} instance types?",
        default=True
    )

    if use_defaults:
        default_types = get_default_instance_types(cloud)
        download_node = default_types['download_node_type']
        etl_node = default_types['etl_node_type']
        analysis_node = default_types['analysis_node_type']
    else:
        download_node = prompt_with_default(
            "  Download cluster node type (64 cores)",
            compute.get('download_node_type', 'r5d.16xlarge')
        )

        etl_node = prompt_with_default(
            "  ETL cluster node type (8 cores, memory optimized)",
            compute.get('etl_node_type', 'r5d.2xlarge')
        )

        analysis_node = prompt_with_default(
            "  Analysis cluster node type (4 cores)",
            compute.get('analysis_node_type', 'r5d.xlarge')
        )

    # Build configuration
    config = {
        "lakehouse": {
            "catalog": catalog,
            "schema": schema,
            "volume": volume
        },
        "api_paths": {
            "cases_endpt": "https://api.gdc.cancer.gov/cases",
            "files_endpt": "https://api.gdc.cancer.gov/files",
            "data_endpt": "https://api.gdc.cancer.gov/data/"
        },
        "pipeline": {
            "max_workers": int(max_workers),
            "max_records": int(max_records),
            "force_download": False,
            "retry_attempts": 3,
            "timeout_seconds": 300
        },
        "compute": {
            "download_node_type": download_node,
            "etl_node_type": etl_node,
            "analysis_node_type": analysis_node
        },
        "deployment": {
            "profile": profile,
            "cloud": cloud
        }
    }

    return config


def save_config(config: Dict[str, Any], config_path: Path):
    """Save configuration to config.json"""
    try:
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=2)
        print_success(f"Configuration saved to {config_path}")
    except Exception as e:
        print_error(f"Failed to save configuration: {e}")
        sys.exit(1)


def update_databricks_yml(config: Dict[str, Any], repo_root: Path):
    """Update databricks.yml with configuration values"""
    try:
        yml_path = repo_root / 'databricks.yml'

        # Read current content
        with open(yml_path, 'r') as f:
            content = f.read()

        # Update profile in workspace section
        profile = config['deployment']['profile']
        if 'profile:' in content:
            # Replace existing profile
            import re
            content = re.sub(
                r'profile: \w+',
                f'profile: {profile}',
                content
            )

        # Note: We don't update default values in databricks.yml anymore
        # since we'll pass them as --var flags during deployment

        with open(yml_path, 'w') as f:
            f.write(content)

        print_success("Updated databricks.yml")
    except Exception as e:
        print_warning(f"Could not update databricks.yml: {e}")


def validate_databricks_cli():
    """Validate Databricks CLI is installed and configured"""
    try:
        result = subprocess.run(
            ['databricks', '--version'],
            capture_output=True,
            text=True,
            check=True
        )
        print_success(f"Databricks CLI detected: {result.stdout.strip()}")
        return True
    except FileNotFoundError:
        print_error("Databricks CLI not found. Please install it first:")
        print("  pip install databricks-cli")
        return False
    except subprocess.CalledProcessError:
        print_error("Databricks CLI is not working correctly")
        return False


def create_unity_catalog_resources(config: Dict[str, Any]) -> bool:
    """Create Unity Catalog resources (catalog, schema, volume)"""
    try:
        print_header("Creating Unity Catalog Resources")

        catalog = config['lakehouse']['catalog']
        schema = config['lakehouse']['schema']
        volume = config['lakehouse']['volume']
        profile = config['deployment']['profile']

        # Check if catalog exists
        print_info(f"Checking catalog: {catalog}")
        result = subprocess.run(
            ['databricks', 'catalogs', 'get', catalog, '--profile', profile],
            capture_output=True,
            text=True
        )

        if result.returncode != 0:
            print_warning(f"Catalog '{catalog}' not found or not accessible")
            print_info("Note: Creating a catalog requires METASTORE_ADMIN privilege")

            create_catalog = prompt_yes_no(f"  Try to create catalog '{catalog}'?", default=False)
            if create_catalog:
                result = subprocess.run(
                    ['databricks', 'catalogs', 'create', catalog, '--profile', profile],
                    capture_output=True,
                    text=True
                )
                if result.returncode != 0:
                    print_error(f"Failed to create catalog: {result.stderr}")
                    print_info("You may need to ask your workspace admin to create it or grant you permissions")
                    return False
                print_success(f"Catalog '{catalog}' created")
            else:
                print_error("Cannot proceed without catalog")
                return False
        else:
            print_success(f"Catalog '{catalog}' exists")

        # Create schema
        print_info(f"Creating schema {catalog}.{schema}...")
        result = subprocess.run(
            [
                'databricks', 'schemas', 'create',
                f'{catalog}.{schema}',
                '--profile', profile
            ],
            capture_output=True,
            text=True
        )

        if result.returncode != 0:
            if 'already exists' in result.stderr.lower():
                print_success(f"Schema {catalog}.{schema} already exists")
            else:
                print_warning(f"Could not create schema: {result.stderr}")
                print_info("The schema may already exist or you may lack permissions")
        else:
            print_success(f"Schema {catalog}.{schema} created")

        # Create volume
        print_info(f"Creating volume {catalog}.{schema}.{volume}...")
        result = subprocess.run(
            [
                'databricks', 'volumes', 'create',
                catalog,
                schema,
                volume,
                'MANAGED',
                '--profile', profile
            ],
            capture_output=True,
            text=True
        )

        if result.returncode != 0:
            if 'already exists' in result.stderr.lower():
                print_success(f"Volume {catalog}.{schema}.{volume} already exists")
            else:
                print_error(f"Could not create volume: {result.stderr}")
                print_warning("\nPlease create the volume manually:")
                print(f"  1. Go to Databricks SQL Editor or Notebook")
                print(f"  2. Run: CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")

                if not prompt_yes_no("\nContinue with deployment anyway?", default=False):
                    return False
        else:
            print_success(f"Volume {catalog}.{schema}.{volume} created")

        print_success("Unity Catalog resources ready!")
        return True

    except Exception as e:
        print_error(f"Error creating Unity Catalog resources: {e}")
        print_warning("\nYou can create them manually in a Databricks notebook:")
        print(f"  spark.sql('CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}')")
        print(f"  spark.sql('CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}')")

        if prompt_yes_no("\nContinue with deployment anyway?", default=False):
            return True
        return False


def deploy_bundle(config: Dict[str, Any], repo_root: Path) -> bool:
    """Deploy the Databricks bundle"""
    try:
        print_header("Deploying Pipeline")

        # Get username for run_as configuration
        profile = config['deployment']['profile']
        username = get_current_username(profile)

        if not username:
            print_error("Could not determine Databricks username")
            print_info("The DLT pipeline needs to run as a specific user for volume permissions")
            username = input("Please enter your Databricks username (e.g., user@company.com): ").strip()
            if not username:
                print_error("Username is required for deployment")
                return False

        print_success(f"DLT pipeline will run as: {username}")

        # Build var flags for deployment
        var_flags = [
            '--var', f'catalog_name={config["lakehouse"]["catalog"]}',
            '--var', f'schema_name={config["lakehouse"]["schema"]}',
            '--var', f'volume_name={config["lakehouse"]["volume"]}',
            '--var', f'user_name={username}',
            '--var', f'max_workers={config["pipeline"]["max_workers"]}',
            '--var', f'download_node_type={config["compute"]["download_node_type"]}',
            '--var', f'etl_node_type={config["compute"]["etl_node_type"]}',
            '--var', f'analysis_node_type={config["compute"]["analysis_node_type"]}'
        ]

        # Validate bundle
        print_info("Validating bundle configuration...")
        result = subprocess.run(
            ['databricks', 'bundle', 'validate', '--target', 'dev'] + var_flags,
            cwd=repo_root,
            capture_output=True,
            text=True
        )

        if result.returncode != 0:
            print_error("Bundle validation failed:")
            print(result.stderr)
            return False

        print_success("Bundle validation passed")

        # Deploy bundle
        print_info("Deploying bundle to Databricks workspace...")
        result = subprocess.run(
            ['databricks', 'bundle', 'deploy', '--target', 'dev'] + var_flags,
            cwd=repo_root,
            capture_output=False,  # Show output in real-time
            text=True
        )

        if result.returncode != 0:
            print_error("Bundle deployment failed")
            return False

        print_success("Bundle deployed successfully!")

        # Get deployment info
        result = subprocess.run(
            ['databricks', 'bundle', 'summary', '--target', 'dev'],
            cwd=repo_root,
            capture_output=True,
            text=True
        )

        if result.returncode == 0:
            print("\n" + Colors.BOLD + "Deployment Summary:" + Colors.ENDC)
            print(result.stdout)

        return True

    except Exception as e:
        print_error(f"Deployment failed: {e}")
        return False


def run_workflow(repo_root: Path) -> bool:
    """Run the deployed workflow"""
    try:
        print_header("Running Workflow")

        print_info("Starting TCGA data workflow...")
        result = subprocess.run(
            ['databricks', 'bundle', 'run', 'tcga_data_workflow', '--target', 'dev'],
            cwd=repo_root,
            capture_output=False,  # Show output in real-time
            text=True
        )

        if result.returncode != 0:
            print_error("Workflow execution failed")
            return False

        print_success("Workflow started successfully!")
        return True

    except Exception as e:
        print_error(f"Failed to run workflow: {e}")
        return False


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Deploy TCGA data pipeline to Databricks',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python deploy.py                  # Interactive setup and deploy
  python deploy.py --run            # Deploy and run immediately
  python deploy.py --config-only    # Only create config.json
  python deploy.py --non-interactive # Use existing config.json (fail if not found)
        """
    )

    parser.add_argument(
        '--config-only',
        action='store_true',
        help='Only create/update config.json without deploying'
    )

    parser.add_argument(
        '--deploy-only',
        action='store_true',
        help='Deploy without running the workflow'
    )

    parser.add_argument(
        '--run',
        action='store_true',
        help='Deploy and run the workflow immediately'
    )

    parser.add_argument(
        '--non-interactive',
        action='store_true',
        help='Use existing config.json without prompts (fail if not found)'
    )

    parser.add_argument(
        '--config',
        type=str,
        default='config.json',
        help='Path to config file (default: config.json)'
    )

    args = parser.parse_args()

    # Get repository root
    repo_root = Path(__file__).parent
    config_path = repo_root / args.config

    print_header("TCGA Pipeline Deployment Tool")

    # Validate Databricks CLI
    if not validate_databricks_cli():
        sys.exit(1)

    # Load or collect configuration
    existing_config = load_existing_config(config_path)

    if args.non_interactive:
        if not existing_config:
            print_error(f"No config file found at {config_path}")
            print_info("Run without --non-interactive to create configuration")
            sys.exit(1)
        config = existing_config
        print_success(f"Loaded configuration from {config_path}")
    else:
        if existing_config:
            print_success(f"Found existing configuration at {config_path}")
            use_existing = prompt_yes_no(
                "Use existing configuration?",
                default=True
            )
            if use_existing:
                config = existing_config
            else:
                config = collect_configuration(existing_config)
                save_config(config, config_path)
        else:
            config = collect_configuration()
            save_config(config, config_path)

    # Display configuration summary
    print("\n" + Colors.BOLD + "Configuration Summary:" + Colors.ENDC)
    print(f"  Catalog: {config['lakehouse']['catalog']}")
    print(f"  Schema: {config['lakehouse']['schema']}")
    print(f"  Volume: {config['lakehouse']['volume']}")
    print(f"  Profile: {config['deployment']['profile']}")
    print(f"  Cloud: {config['deployment']['cloud'].upper()}")
    print(f"  Download node: {config['compute']['download_node_type']}")
    print(f"  ETL node: {config['compute']['etl_node_type']}")
    print(f"  Analysis node: {config['compute']['analysis_node_type']}")

    if args.config_only:
        print_success("Configuration complete!")
        return

    # Update databricks.yml
    update_databricks_yml(config, repo_root)

    # Create Unity Catalog resources
    if not create_unity_catalog_resources(config):
        print_error("Failed to create Unity Catalog resources")
        sys.exit(1)

    # Deploy bundle
    if not deploy_bundle(config, repo_root):
        sys.exit(1)

    # Run workflow if requested
    if args.run and not args.deploy_only:
        print()
        run_now = prompt_yes_no(
            "Run the workflow now?",
            default=True
        )
        if run_now:
            run_workflow(repo_root)

    print_header("Deployment Complete!")
    print_info("Next steps:")
    print("  1. Monitor workflows: databricks bundle run tcga_data_workflow --target dev")
    print("  2. View logs in Databricks UI")
    print(f"  3. Data will be stored in: {config['lakehouse']['catalog']}.{config['lakehouse']['schema']}")


if __name__ == '__main__':
    main()
