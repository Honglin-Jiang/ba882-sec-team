from prefect import flow
import os

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
                "pip_packages": ["pandas", "requests", "google-cloud", "google-cloud-secret-manager"]
            }
        )
    except Exception as e:
        print(f"Error during deployment: {str(e)}")
        raise e
