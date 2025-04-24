import os
from subprocess import run

# Create a dbt project directory
os.makedirs("dbt_project", exist_ok=True)
os.chdir("dbt_project")
# Initialize dbt project (will prompt for adapter choice)
run(["dbt", "init", "--profiles-dir", "."], check=True)