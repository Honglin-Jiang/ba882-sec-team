from main import run_forecasting

if __name__ == "__main__":
    run_forecasting.serve(
        name="stock-forecasting-deployment",
        cron="0 21 * * *",  
        tags=["stock-forecasting"],
        description="Daily stock price forecasting pipeline"
    )