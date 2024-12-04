'''
from prefect import flow

if __name__ == "__main__":
    try:

        etl_deployment = flow.from_source(
            source="https://github.com/Honglin-Jiang/ba882-sec-team.git",
            entrypoint="flows/etl.py:etl_flow",
        )

        # Deploy with required pip packages
        etl_deployment.deploy(
            name="ba882-team9-deployment-lab6",
            work_pool_name="ba882-team9-schedule",
            cron="17 17 * * *",
            tags=["daily-run"],
            description="Pipeline to extract data from YFinance API and MD&A filing, transform, and store it into Motherduck DB",
            version="1.0.0",
            job_variables={
                "env": {},
                "pip_packages": ["pandas", "requests", "google-cloud", "google-cloud-secret-manager"]
            }
        )
    except Exception as e:
        print(f"Error during deployment: {str(e)}")
        raise e
'''

'''
from prefect import flow
from prefect.deployments import Deployment
import os


# Define the main ETL flow (make sure etl_flow is defined in flows/etl.py)
@flow
def etl_flow():
    # Logic for the ETL process goes here...
    pass

if __name__ == "__main__":
    try:
        # Build the deployment from the GitHub source file
        etl_deployment = Deployment.build_from_flow(
            flow=etl_flow,
            name="ba882-team9-deployment-lab6",
            work_pool_name="ba882-team9-schedule",
            schedule="cron(17 17 * * *)",
            tags=["daily-run"],
            description="Pipeline to extract data from YFinance API and MD&A filing, transform, and store it into Motherduck DB",
            version="1.0.0",
            parameters={},
            job_variables={
                "env": {},
                "pip_packages": ["pandas", "requests", "google-cloud", "google-cloud-secret-manager"]
            }
        )

        # Apply the deployment
        etl_deployment.apply()

    except Exception as e:
        print(f"Error during deployment: {str(e)}")
        raise e
'''

from prefect import flow
import os
from google.cloud import secretmanager

project_id = "ba882-team9"
secret_id = "prefect-api-key"
version_id = "latest"

def get_secret(project_id, secret_id, version_id="latest"):
    # Instantiate the Secret Manager service
    sm = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret version
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret version
    response = sm.access_secret_version(request={"name": name})
    secret_data = response.payload.data.decode("UTF-8")

    return secret_data

# Set Prefect API key and workspace
PREFECT_API_KEY = get_secret(project_id, secret_id, version_id)
PREFECT_API_URL = "https://app.prefect.cloud/account/db2be91e-ac3c-47c7-8afc-9f1f043c2027/workspace/2d46f809-2085-4220-97da-e282ded15dc7"
 
if _name_ == "_main_":
    try:
        # Configure Prefect Cloud connection
        os.environ["PREFECT_API_URL"] = PREFECT_API_URL
        
        # Login to Prefect Cloud
        os.system(f'prefect cloud login --key {PREFECT_API_KEY} --workspace ""ba-882/default/2d46f809-2085-4220-97da-e282ded15dc7"')
        
        # Create work pool if it doesn't exist
        os.system('prefect work-pool create ba882-team9-schedule --type process')
        
        # Deploy the flow
        flow.from_source(
            source="https://github.com/Honglin-Jiang/ba882-sec-team.git",
            entrypoint="flows/etl.py:etl_flow",
        ).deploy(
            name="daily-etl",
            work_pool_name="ba882-team9-schedule",
            job_variables={
                "env": {},
                "pip_packages": ["pandas", "requests"]
            },
            cron="00 12 * * *", # New York Time 7PM/8PM
            tags=["prod"],
            description="Pipeline to extract data from YFinance API and MD&A filing, transform, and store it into Motherduck DB.",
            version="1.0.0",
        )

        # Deploy the flow - test
        flow.from_source(
            source="https://github.com/Honglin-Jiang/ba882-sec-team.git",
            entrypoint="flows/etl.py:etl_flow",
        ).deploy(
            name="daily-etl-test",
            work_pool_name="ba882-team9-schedule",
            job_variables={
                "env": {},
                "pip_packages": ["pandas", "requests"]
            },
            cron="30 3 * * *", # New York Time 9:30PM/10:00PM
            tags=["prod"],
            description="Pipeline to extract data from YFinance API and MD&A filing, transform, and store it into Motherduck DB. - test version",
            version="1.0.0",
        )
    except Exception as e:
        print(f"Error during deployment: {str(e)}")
        raise e