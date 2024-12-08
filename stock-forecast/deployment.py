from main import run_forecasting
from prefect.server.schemas.schedules import CronSchedule

if __name__ == "__main__":
    run_forecasting.serve(
        name="stock-forecasting-deployment",
        cron="0 0 * * *",  # Run daily at midnight
        tags=["stock-forecasting"],
        parameters={},
        interval_seconds=None,
        description="Daily stock price forecasting pipeline"
    )