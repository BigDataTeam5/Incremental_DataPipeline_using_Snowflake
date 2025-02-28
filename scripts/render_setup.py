import os
import sys
import yaml
import datetime
from jinja2 import Environment, FileSystemLoader
from dotenv import load_dotenv
load_dotenv('.env')

def render_template(env_name):
    """
    Render the setup.sql template for the specified environment.
    
    Args:
        env_name: Environment name ("dev" or "prod")
    """
    # Load environment variables for AWS credentials
    load_dotenv('.env')
    
    # Set paths
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))  # Moves up one level from 'scripts'
    config_file = os.path.join(base_dir, "config", f"{env_name}.yml")
    template_file = os.path.join(base_dir, "templates", "setup.sql.j2")
    output_file = os.path.join(base_dir, "scripts", f"setup_{env_name}.sql")    # Ensure the template file exists
    if not os.path.exists(template_file):
        raise FileNotFoundError(f"Template file not found: {template_file}")
    
    # Load environment configuration
    with open(config_file, "r") as f:
        config = yaml.safe_load(f)
    
    # Set up Jinja environment
    template_dir = os.path.dirname(template_file)
    jinja_env = Environment(loader=FileSystemLoader(template_dir))
    template = jinja_env.get_template(os.path.basename(template_file))
    
    # Add current date
    current_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Render the template
    rendered_sql = template.render(
        env=env_name,
        config=config,
        aws_access_key=os.getenv("AWS_ACCESS_KEY"),
        aws_secret_key=os.getenv("AWS_SECRET_KEY"),
        current_date=current_date
    )
    
    # Write the rendered SQL to a file
    with open(output_file, "w") as f:
        f.write(rendered_sql)
    
    print(f"Generated setup script for {env_name.upper()} environment: {output_file}")

if __name__ == "__main__":
    # Default to "dev" if no environment is specified
    env_name = "dev"
    
    # Allow specifying environment as command-line argument
    if len(sys.argv) > 1:
        env_name = sys.argv[1].lower()
        if env_name not in ["dev", "prod"]:
            print(f"Invalid environment: {env_name}. Using 'dev' instead.")
            env_name = "dev"
    
    render_template(env_name)