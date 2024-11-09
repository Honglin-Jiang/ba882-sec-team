# forecast_task.py
import pandas as pd
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.statespace.sarimax import SARIMAX
import datetime
import uuid
import functions_framework
from md_utils import connect_to_motherduck

# Define your database and schema
db = 'ba882_team9'
schema = "stage"
db_schema = f"{db}.{schema}"

@functions_framework.http
def forecast_and_store(request):
    try:
        # Establish a connection to the MotherDuck database
        conn = connect_to_motherduck()
        
        # Fetch the data from the MotherDuck database
        df = conn.execute("SELECT * FROM ba882_team9.transformed.y_finance").fetchdf()

        # Initialize an empty DataFrame to store forecast results
        predictions_df = pd.DataFrame(columns=['ticker', 'date', 'arima_forecast', 'sarima_forecast'])

        # Loop through each unique ticker and perform the forecasting
        for ticker in df['ticker'].unique():
            # Filter the data for the last 2 years for each stock
            stock_df = df[df['ticker'] == ticker].last("2Y")
            
            # Train ARIMA model
            model_arima = ARIMA(stock_df['close'], order=(5, 1, 0))
            arima_result = model_arima.fit()
            forecast_arima = arima_result.forecast(steps=1)
            
            # Train SARIMA model
            model_sarima = SARIMAX(stock_df['close'], order=(1, 1, 1), seasonal_order=(1, 1, 1, 12))
            sarima_result = model_sarima.fit()
            forecast_sarima = sarima_result.forecast(steps=1)
            
            # Calculate the next day's date
            next_day = stock_df.index[-1] + pd.Timedelta(days=1)
            
            # Append the result to predictions_df
            predictions_df = predictions_df.append({
                'ticker': ticker,
                'date': next_day,
                'arima_forecast': forecast_arima.iloc[0],
                'sarima_forecast': forecast_sarima.iloc[0]
            }, ignore_index=True)

        # Insert the forecast data into the MotherDuck database
        for _, row in predictions_df.iterrows():
            insert_sql = f"""
                INSERT INTO {db_schema}.stock_forecasts (ticker, date, arima_forecast, sarima_forecast)
                VALUES ('{row['ticker']}', '{row['date']}', {row['arima_forecast']}, {row['sarima_forecast']});
            """
            conn.execute(insert_sql)

        # Close the database connection
        conn.close()
        return {}, 200

    except Exception as e:
        print(f"Error occurred: {e}")
        return {"error": str(e)}, 500
