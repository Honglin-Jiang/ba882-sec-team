import os
import pandas as pd
import duckdb
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.statespace.sarimax import SARIMAX
from datetime import datetime, timedelta
from prefect import flow, task
from prefect.logging import get_run_logger
from flask import Flask, jsonify
import logging
import traceback

app = Flask(__name__)

# Enhanced logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
MOTHERDUCK_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6InlmbGlhb0BidS5lZHUiLCJzZXNzaW9uIjoieWZsaWFvLmJ1LmVkdSIsInBhdCI6IlFoYVh2U3BxM2g5eW0zbkRQMkV1a3l3cGk0ZVdpNWNhbGhNdE03ZkxpNUUiLCJ1c2VySWQiOiI5ZmY0NzMzYS1lMmQzLTRhOTktYTI0Zi04YjJhMTk3YWQ3MDIiLCJpc3MiOiJtZF9wYXQiLCJyZWFkT25seSI6ZmFsc2UsImlhdCI6MTczMTQyNzA5OCwiZXhwIjoxNzM2NjExMDk4fQ._zrg6_5phqbqVnJCKv5rRq-3yLwRc2ZZOHFbZTE2t-o"
DB_SCHEMA = "ba882_team9.transformed"

# Target tech stocks
TICKERS = ['INTC', 'AMD', 'MSFT', 'BABA', 'NVDA', 'TSM', 'CRM', 'GOOG', 'AMZN']

@task(retries=3, retry_delay_seconds=60)
def connect_to_motherduck():
    """Connect to MotherDuck database."""
    task_logger = get_run_logger()
    try:
        conn = duckdb.connect(f"md:?token={MOTHERDUCK_TOKEN}")
        task_logger.info("Successfully connected to MotherDuck")
        return conn
    except Exception as e:
        task_logger.error(f"Failed to connect to MotherDuck: {e}")
        raise

@task
def fetch_stock_data(conn):
    """Fetch stock data from MotherDuck."""
    task_logger = get_run_logger()
    try:
        two_years_ago = (datetime.now() - timedelta(days=2*365)).strftime('%Y-%m-%d')
        tickers_str = ", ".join([f"'{ticker}'" for ticker in TICKERS])
        query = f"""
        SELECT * 
        FROM {DB_SCHEMA}.y_finance 
        WHERE ticker IN ({tickers_str})
        AND date >= '{two_years_ago}'
        ORDER BY date ASC;
        """
        task_logger.info("Executing stock data query")
        return conn.sql(query).fetchdf()
    except Exception as e:
        task_logger.error(f"Error fetching stock data: {e}")
        raise

@task
def prepare_data(df):
    """Prepare the data for forecasting."""
    task_logger = get_run_logger()
    try:
        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date').sort_index()
        return df
    except Exception as e:
        task_logger.error(f"Error preparing data: {e}")
        raise

@task(retries=2)
def forecast_stock(stock_df, ticker):
    """Generate forecasts for a single stock."""
    task_logger = get_run_logger()
    try:
        # Train ARIMA model
        model_arima = ARIMA(stock_df['close'], order=(5, 1, 0))
        arima_result = model_arima.fit()
        forecast_arima = arima_result.forecast(steps=1)

        # Train SARIMA model
        model_sarima = SARIMAX(stock_df['close'], 
                              order=(1, 1, 1), 
                              seasonal_order=(1, 1, 1, 12))
        sarima_result = model_sarima.fit(disp=False)
        forecast_sarima = sarima_result.forecast(steps=1)

        next_day = stock_df.index[-1] + pd.Timedelta(days=1)
        
        task_logger.info(f"Generated forecast for {ticker}")
        return {
            'ticker': ticker,
            'date': next_day,
            'arima_forecast': float(forecast_arima.iloc[0]),
            'sarima_forecast': float(forecast_sarima.iloc[0])
        }
    except Exception as e:
        task_logger.error(f"Error forecasting {ticker}: {e}")
        raise

@task
def save_forecasts(conn, predictions):
    """Save forecasts to MotherDuck."""
    task_logger = get_run_logger()
    try:
        # Create table if it doesn't exist
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {DB_SCHEMA}.stock_forecasts (
            ticker VARCHAR,
            date DATE,
            arima_forecast DOUBLE,
            sarima_forecast DOUBLE
        );
        """
        conn.execute(create_table_sql)

        # Insert predictions
        for pred in predictions:
            insert_sql = f"""
            INSERT INTO {DB_SCHEMA}.stock_forecasts 
            (ticker, date, arima_forecast, sarima_forecast)
            VALUES 
            ('{pred['ticker']}', '{pred['date']}', 
             {pred['arima_forecast']}, {pred['sarima_forecast']});
            """
            conn.execute(insert_sql)
            task_logger.info(f"Saved forecast for {pred['ticker']}")
    except Exception as e:
        task_logger.error(f"Error saving forecasts: {e}")
        raise

@flow(name="stock_forecasting")
def run_forecasting():
    """Run the complete forecasting process."""
    flow_logger = get_run_logger()
    flow_logger.info("Starting forecasting flow")
    try:
        conn = connect_to_motherduck()
        
        # Fetch and prepare data
        df = fetch_stock_data(conn)
        df = prepare_data(df)
        
        # Generate forecasts
        predictions = []
        for ticker in TICKERS:
            stock_df = df[df['ticker'] == ticker]
            if not stock_df.empty:
                forecast = forecast_stock(stock_df, ticker)
                predictions.append(forecast)
        
        # Save results
        save_forecasts(conn, predictions)
        flow_logger.info("Forecasting flow completed successfully")
        return "Forecasting completed successfully", 200
    except Exception as e:
        flow_logger.error(f"Error during forecast processing: {e}")
        return f"Error during forecast processing: {str(e)}", 500

@app.route('/', methods=['GET'])
def home():
    return jsonify({"status": "healthy", "message": "Stock Forecasting Service is running"})

@app.route('/forecast', methods=['POST'])
def forecast_endpoint():
    try:
        logger.info("Starting forecast endpoint")
        result = run_forecasting()
        logger.info(f"Forecast completed with result: {result}")
        return jsonify({"message": "Forecast process completed", "result": result}), 200
    except Exception as e:
        error_details = traceback.format_exc()
        logger.error(f"Error in forecast endpoint: {str(e)}\nTraceback: {error_details}")
        return jsonify({
            "error": str(e),
            "details": error_details,
            "message": "Error processing forecast"
        }), 500

if __name__ == "__main__":
    try:
        port = int(os.environ.get('PORT', 8080))
        app.run(host='0.0.0.0', port=port, debug=False)
    except Exception as e:
        logger.error(f"Error starting server: {e}")