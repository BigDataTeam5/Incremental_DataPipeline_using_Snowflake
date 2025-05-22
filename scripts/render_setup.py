import os
import sys
import yaml
import datetime
from jinja2 import Environment, FileSystemLoader
from dotenv import load_dotenv
import json

def render_templates(env_name):
    """
    Render all templates for the specified environment.
    
    Args:
        env_name: Environment name ("dev" or "prod")
    """
    # Load environment variables for AWS credentials
    load_dotenv('.env')
    
    # Set paths
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))  # Moves up one level from 'scripts'
    config_file = os.path.join(base_dir, "config", f"{env_name}.yml")
    templates_dir = os.path.join(base_dir, "templates")
    scripts_dir = os.path.join(base_dir, "scripts")
    
    # Load environment configuration
    with open(config_file, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    
    # Set up Jinja environment
    jinja_env = Environment(loader=FileSystemLoader(templates_dir))
    
    # Add current date
    current_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Define templates to render
    templates_to_render = [
        {
            "template": "setup.sql.j2",
            "output": f"setup_{env_name}.sql"
        },
        {
            "template": "orchestrate_tasks.sql.j2",
            "output": f"orchestrate_tasks_{env_name}.sql"
        },
        {
            "template": "table_grants.sql.j2",
            "output": f"table_grants_{env_name}.sql"
        }
    ]
    
    # Render each template
    for template_info in templates_to_render:
        template_file = template_info["template"]
        output_file = os.path.join(scripts_dir, template_info["output"])
        
        # Ensure the template file exists
        if not os.path.exists(os.path.join(templates_dir, template_file)):
            raise FileNotFoundError(f"Template file not found: {template_file}")
        
        template = jinja_env.get_template(template_file)
        
        # Render the template with context
        rendered_content = template.render(
            env=env_name,
            config=config,
            aws_access_key=os.getenv("AWS_ACCESS_KEY"),
            aws_secret_key=os.getenv("AWS_SECRET_KEY"),
            current_date=current_date
        )
        
        # Write the rendered content to the output file
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(rendered_content)
        
        print(f"Generated {template_info['output']} for {env_name.upper()} environment")

if __name__ == "__main__":
    # Get the base directory (project root)
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    env_file_path = os.path.join(base_dir, "templates", "environment.json")
    
    try:
        # Read environment from JSON file
        with open(env_file_path, "r") as json_file:
            env_config = json.load(json_file)
            env_name = env_config.get("environment", "").lower()
            
        if not env_name:
            raise ValueError("Environment not set in environment.json")
            
        print(f"Using environment from file: {env_name}")
        render_templates(env_name)
        
    except FileNotFoundError:
        print(f"ERROR: environment.json not found at {env_file_path}")
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"ERROR: Invalid JSON in environment.json")
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: {str(e)}")
        sys.exit(1)