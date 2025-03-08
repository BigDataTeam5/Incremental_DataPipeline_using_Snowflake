#!/usr/bin/env python3
"""
Force deployment of specific UDFs/procedures by creating a commit that touches the files.
"""
import os
import sys
import argparse
import subprocess
import datetime
from pathlib import Path

def get_repo_root():
    """Get the repository root directory."""
    try:
        cmd = ["git", "rev-parse", "--show-toplevel"]
        return subprocess.check_output(cmd).decode().strip()
    except subprocess.CalledProcessError:
        print("Error: Not a git repository or git command failed.")
        sys.exit(1)

def force_deploy_component(component_type, component_name, commit=False):
    """
    Force deployment of a specific component by adding a touch file.
    
    Args:
        component_type: Either "udf" or "procedure"
        component_name: Name of the component to deploy
        commit: Whether to commit the change automatically
    """
    repo_root = Path(get_repo_root())
    base_path = repo_root / "udfs_and_spoc"
    
    # Map component names to directory paths
    component_map = {
        "udf": {
            "co2_volatility": "python_udf",
            "daily_co2_changes": "daily_co2_changes",
            "weekly_co2_changes": "weekly_co2_changes",
        },
        "procedure": {
            "load_co2_data": "loading_co2_data_sp",
            "harmonize_co2_data": "co2_harmonized_sp",
            "analyze_co2_data": "co2_analytical_sp",
        }
    }
    
    # Determine the component path
    component_type_lower = component_type.lower()
    component_name_lower = component_name.lower()
    
    if component_type_lower not in component_map:
        print(f"Error: Unknown component type '{component_type}'")
        print(f"Valid types are: {', '.join(component_map.keys())}")
        sys.exit(1)
    
    if component_name_lower not in component_map[component_type_lower]:
        print(f"Error: Unknown {component_type_lower} '{component_name}'")
        print(f"Valid {component_type_lower} names are: {', '.join(component_map[component_type_lower].keys())}")
        sys.exit(1)
    
    # Get directory name for the component
    dir_name = component_map[component_type_lower][component_name_lower]
    component_path = base_path / dir_name
    
    # Ensure the path exists
    if not component_path.exists():
        print(f"Error: Component directory not found: {component_path}")
        sys.exit(1)
    
    # Create a force_deploy file with timestamp
    force_file = component_path / ".force_deploy"
    with open(force_file, "w") as f:
        timestamp = datetime.datetime.now().isoformat()
        f.write(f"Force deployment triggered at {timestamp}\n")
        f.write(f"Component: {component_name}\n")
        f.write(f"Type: {component_type}\n")
    
    print(f"✅ Created {force_file} to force deployment of {component_type} '{component_name}'")
    
    # Commit the change if requested
    if commit:
        try:
            subprocess.run(["git", "add", str(force_file)], check=True)
            subprocess.run(["git", "commit", "-m", f"Force deploy {component_type} '{component_name}'"], check=True)
            print("✅ Committed the force deploy file")
            print("ℹ️ Push this commit to trigger deployment in CI/CD pipeline")
        except subprocess.CalledProcessError:
            print("❌ Failed to commit the force deploy file. You'll need to commit it manually.")
    else:
        print("ℹ️ Now commit and push this change to trigger deployment in CI/CD pipeline:")
        print(f"  git add {force_file}")
        print(f"  git commit -m \"Force deploy {component_type} '{component_name}'\"")
        print("  git push")

def list_components():
    """List all available components that can be force deployed."""
    components = {
        "UDFs": [
            "CO2_VOLATILITY",
            "DAILY_CO2_CHANGES",
            "WEEKLY_CO2_CHANGES"
        ],
        "Procedures": [
            "LOAD_CO2_DATA",
            "HARMONIZE_CO2_DATA",
            "ANALYZE_CO2_DATA"
        ]
    }
    
    print("Available components for force deploy:")
    print("\nUDFs:")
    for udf in components["UDFs"]:
        print(f"  - {udf}")
    
    print("\nProcedures:")
    for proc in components["Procedures"]:
        print(f"  - {proc}")
    
    print("\nExample usage:")
    print("  python scripts/force_deploy.py --type udf --name CO2_VOLATILITY")
    print("  python scripts/force_deploy.py --type procedure --name LOAD_CO2_DATA --commit")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Force deployment of specific UDFs or procedures')
    parser.add_argument('--type', choices=['udf', 'procedure'], help='Component type')
    parser.add_argument('--name', help='Component name (e.g., CO2_VOLATILITY)')
    parser.add_argument('--commit', action='store_true', help='Automatically commit the force deploy file')
    parser.add_argument('--list', action='store_true', help='List all available components')
    
    args = parser.parse_args()
    
    if args.list:
        list_components()
    elif args.type and args.name:
        force_deploy_component(args.type, args.name, args.commit)
    else:
        parser.print_help()
