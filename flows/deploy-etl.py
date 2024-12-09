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
