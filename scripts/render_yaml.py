import os
import sys
import yaml
from string import Template
import glob

# Get environment from command line or use default
env = sys.argv[1] if len(sys.argv) > 1 else "dev"

# Set paths
base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
config_file = os.path.join(base_dir, "config", f"{env}.yml")

# Load environment config
try:
    with open(config_file, "r") as f:
        config = yaml.safe_load(f)
        print(f"Loaded configuration from {config_file}")
except FileNotFoundError:
    print(f"Error: Config file not found: {config_file}")
    sys.exit(1)
except yaml.YAMLError as e:
    print(f"Error parsing YAML: {e}")
    sys.exit(1)

# Extract values from config
database_name = config.get("database_name", "CO2_DB_DEV")
role_name = config.get("role_name", "CO2_ROLE")
warehouse_name = config.get("warehouse_name", "CO2_WH")

# Define template files to process
template_files = [
    {
        "template": os.path.join(base_dir, "scripts/daily_co2_changes/snowflake.yml.template"),
        "output": os.path.join(base_dir, "scripts/daily_co2_changes/snowflake.yml")
    },
    {
        "template": os.path.join(base_dir, "scripts/weekly_co2_changes/snowflake.yml.template"),
        "output": os.path.join(base_dir, "scripts/weekly_co2_changes/snowflake.yml")
    },

    {
        "template": os.path.join(base_dir, "scripts/python_udf/snowflake.yml.template"),
        "output": os.path.join(base_dir, "scripts/python_udf/snowflake.yml")
    },
    {
        "template": os.path.join(base_dir, "scripts/co2_harmonized_sp/snowflake.yml.template"),
        "output": os.path.join(base_dir, "scripts/co2_harmonized_sp/snowflake.yml")
    },
    {
        "template": os.path.join(base_dir, "scripts/co2_analytical_sp/snowflake.yml.template"),
        "output": os.path.join(base_dir, "scripts/co2_analytical_sp/snowflake.yml")
    }
]

# For each template, replace placeholders and write output
for file_config in template_files:
    template_path = file_config["template"]
    output_path = file_config["output"]
    
    # Check if template file exists
    if not os.path.exists(template_path):
        print(f"Warning: Template file not found: {template_path}")
        continue
    
    # Read the template file
    try:
        with open(template_path, "r") as f:
            template_content = f.read()
    except Exception as e:
        print(f"Error reading template file {template_path}: {e}")
        continue
    
    # Replace the placeholders
    replacements = {
        "{{ DATABASE_NAME }}": database_name,
        "{{ ROLE_NAME }}": role_name,
        "{{ WAREHOUSE_NAME }}": warehouse_name
    }
    
    rendered_content = template_content
    for placeholder, value in replacements.items():
        rendered_content = rendered_content.replace(placeholder, value)
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Write to the output file
    try:
        with open(output_path, "w") as f:
            f.write(rendered_content)
        print(f"Generated {os.path.basename(output_path)} for {env.upper()} environment")
    except Exception as e:
        print(f"Error writing to {output_path}: {e}")

print(f"\nConfiguration values used:")
print(f"  - Database: {database_name}")
print(f"  - Role: {role_name}")
print(f"  - Warehouse: {warehouse_name}")