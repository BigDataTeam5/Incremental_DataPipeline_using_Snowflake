import os
import sys
import yaml
import datetime
from jinja2 import Environment, FileSystemLoader
from dotenv import load_dotenv
import json

def update_environment(env):
    data = {"environment": env}
    with open("templates/environment.json", "w", encoding="utf-8") as json_file:
        json.dump(data, json_file, indent=4)
    print(f"Environment set to: {env}")

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
    if len(sys.argv) != 2:
        print("Usage: poetry run python scripts/render_setup.py <environment>")
        sys.exit(1)
    
    environment = sys.argv[1]
    update_environment(environment)
    render_templates(environment)