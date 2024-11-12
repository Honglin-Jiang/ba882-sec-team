from prefect import flow

if __name__ == "__main__":
    etl_deployment = flow.from_source(
        source="https://github.com/Honglin-Jiang/ba882-sec-team.git",
        entrypoint="flows/etl.py:etl_flow",
    )

    # Deploy with required pip packages
    etl_deployment.deploy(
        name="ba882-team9-deployment-lab6",
        work_pool_name="ba882-team9-schedule",
        cron="45 5 * * *",
        tags=["daily-run"],
        description="Pipeline to extract data from YFinance API and MD&A filing, transform, and store it into Motherduck DB",
        version="1.0.0",
        job_variables={
            "pip_packages": ["pandas", "requests", "google-cloud-secret-manager"]}
    ) 