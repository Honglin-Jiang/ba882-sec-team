'''
from prefect import flow
import os

# Set Prefect API key and workspace
PREFECT_API_KEY = os.getenv('PREFECT_API_KEY')
# PREFECT_API_URL = "https://api.prefect.cloud/api/accounts/[YOUR-ACCOUNT-ID]/workspaces/[YOUR-WORKSPACE-ID]"
PREFECT_API_URL = "https://api.prefect.cloud/api/accounts/db2be91e-ac3c-47c7-8afc-9f1f043c2027/workspaces/2d46f809-2085-4220-97da-e282ded15dc7"
#PREFECT_API_URL = "https://api.prefect.cloud/api/accounts/BU/workspaces/2d46f809-2085-4220-97da-e282ded15dc7"
 

if __name__ == "__main__":
    try:
        # Configure Prefect Cloud connection
        os.environ["PREFECT_API_URL"] = PREFECT_API_URL
        
        # Login to Prefect Cloud
        os.system(f'prefect cloud login --key {PREFECT_API_KEY} --workspace "ba-882/default"')
        
        # Create work pool if it doesn't exist
        os.system('prefect work-pool create ba882-team9-autorun --type process')
        
        # Deploy the flow
        flow.from_source(
            source="https://github.com/Honglin-Jiang/ba882-sec-team.git",
            entrypoint="flows/etl.py:etl_flow",
        ).deploy(
            name="ba882-team9-daily-etl-autorun",
            work_pool_name="ba882-team9-autorun",
            job_variables={
                "env": {},
                "pip_packages": ["pandas", "requests"]
            },
            cron="00 20 * * *",
            tags=["prod"],
            description="Pipeline to extract data from YFinance API and MD&A filing, transform, and store it into Motherduck DB.",
            version="1.0.",
        )
    except Exception as e:
        print(f"Error during deployment: {str(e)}")
        raise e
    
'''


from prefect import flow
if __name__ == "__main__":
    try:
        # Deploy the flow
        flow.from_source(
            source="https://github.com/Honglin-Jiang/ba882-sec-team.git",
            entrypoint="flows/etl.py:etl_flow",
        ).deploy(
            name="ba882-team9-daily-etl-autorun",
            work_pool_name="ba882-team9-autorun",
            job_variables={
                "env": {"ENVIRONMENT":"production"},
                "pip_packages": ["pandas", "requests"]
            },
            cron="15 2 * * *",
            tags=["prod"],
            description="Pipeline to extract data from YFinance API and MD&A filing, transform, and store it into Motherduck DB.",
            version="1.0.",
        )
    except Exception as e:
        print(f"Error during deployment: {str(e)}")
        raise e


'''
import os
from prefect import flow

# Set Prefect API Key and URL
PREFECT_API_KEY = os.getenv("PREFECT_API_KEY")
PREFECT_API_URL = "https://api.prefect.cloud/api/accounts/db2be91e-ac3c-47c7-8afc-9f1f043c2027/workspaces/2d46f809-2085-4220-97da-e282ded15dc7"

# Configure environment variables for Prefect API
os.environ["PREFECT_API_URL"] = PREFECT_API_URL

if __name__ == "__main__":
    try:
        # Ensure PREFECT_API_KEY is set
        if not PREFECT_API_KEY:
            raise EnvironmentError("PREFECT_API_KEY is not set. Please set it as an environment variable.")

        # Login to Prefect Cloud
        print("Logging into Prefect Cloud...")
        os.system(f'prefect cloud login --key {PREFECT_API_KEY} --workspace "ba-882/default"')

        # Create a work pool if it doesn't exist
        print("Creating work pool 'ba882-team9-autorun'...")
        os.system("prefect work-pool create ba882-team9-autorun --type process")

        # Deploy the flow using the correct CLI command
        print("Deploying the flow 'ba882-team9-daily-etl-autorun'...")
        os.system(
            "prefect deploy flows/etl.py:etl_flow "
            "--name ba882-team9-daily-etl-autorun "
            "--pool ba882-team9-autorun "  # Corrected option here
            "--cron '45 8 * * *' "
            "--tag prod "
            "--description 'Pipeline to extract data from YFinance API and MD&A filing, transform, and store it into Motherduck DB.'"
        )

        print("Deployment successfully applied!")

    except Exception as e:
        print(f"Error during deployment: {str(e)}")
        raise

'''