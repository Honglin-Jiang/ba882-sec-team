from prefect import flow

if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/Honglin-Jiang/ba882-sec-team.git",
        entrypoint="flows/etl.py:etl_flow",
    ).deploy(
        name="ba882-team9-deployment-lab6",
        work_pool_name="ba882-team9-schedule",
        cron="0 2 * * *",
        tags=["daily-run"],
        description="The pipeline to extract data daily from YFinance API and MD&A filing, then transform and store the data into Motherduck DB",
        version="1.0.0",
    )