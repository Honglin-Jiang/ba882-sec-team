# simple_forecast.py
import os
import pandas as pd
import duckdb
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.statespace.sarimax import SARIMAX
from datetime import datetime, timedelta

# Configuration
MOTHERDUCK_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6InlmbGlhb0BidS5lZHUiLCJzZXNzaW9uIjoieWZsaWFvLmJ1LmVkdSIsInBhdCI6IlFoYVh2U3BxM2g5eW0zbkRQMkV1a3l3cGk0ZVdpNWNhbGhNdE03ZkxpNUUiLCJ1c2VySWQiOiI5ZmY0NzMzYS1lMmQzLTRhOTktYTI0Zi04YjJhMTk3YWQ3MDIiLCJpc3MiOiJtZF9wYXQiLCJyZWFkT25seSI6ZmFsc2UsImlhdCI6MTczMTQyNzA5OCwiZXhwIjoxNzM2NjExMDk4fQ._zrg6_5phqbqVnJCKv5rRq-3yLwRc2ZZOHFbZTE2t-o"
DB_SCHEMA = "ba882_team9.transformed"  # Updated schema name

# Target tech stocks
TICKERS = ['INTC', 'AMD', 'MSFT', 'BABA', 'NVDA', 'TSM', 'CRM', 'GOOG', 'AMZN']

def connect_to_motherduck():
    """Establish connection to MotherDuck."""
    try:
        conn = duckdb.connect(f"md:?token={MOTHERDUCK_TOKEN}")
        print("Successfully connected to MotherDuck")
        return conn
    except Exception as e:
        print(f"Failed to connect to MotherDuck: {e}")
        return None

def fetch_stock_data(conn):
    """Fetch stock data from MotherDuck."""
    try:
        # Calculate date two years ago
        two_years_ago = (datetime.now() - timedelta(days=2*365)).strftime('%Y-%m-%d')
        
        tickers_str = ", ".join([f"'{ticker}'" for ticker in TICKERS])
        query = f"""
        SELECT * 
        FROM {DB_SCHEMA}.y_finance 
        WHERE ticker IN ({tickers_str})
        AND date >= '{two_years_ago}'
        ORDER BY date ASC;
        """
        print("Executing query:", query)  # Debug print
        return conn.sql(query).fetchdf()
    except Exception as e:
        print(f"Error in fetch_stock_data: {e}")
        print(f"Query attempted: {query}")
        raise

def prepare_data(df):
    """Prepare the data for forecasting."""
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date').sort_index()
    return df

def forecast_stock(stock_df, ticker):
    """Generate forecasts for a single stock."""
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

        return {
            'ticker': ticker,
            'date': next_day,
            'arima_forecast': float(forecast_arima.iloc[0]),
            'sarima_forecast': float(forecast_sarima.iloc[0])
        }
    except Exception as e:
        print(f"Error forecasting {ticker}: {e}")
        return None

def save_forecasts(conn, predictions):
    """Save forecasts to MotherDuck."""
    try:
        # Create table if it doesn't exist
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {DB_SCHEMA}.stock_forecasts (
            ticker VARCHAR,
            date DATE,
            arima_forecast DOUBLE,
            sarima_forecast DOUBLE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        conn.execute(create_table_sql)

        # Insert predictions
        for pred in predictions:
            if pred is not None:
                insert_sql = f"""
                INSERT INTO {DB_SCHEMA}.stock_forecasts 
                (ticker, date, arima_forecast, sarima_forecast)
                VALUES 
                ('{pred['ticker']}', '{pred['date']}', 
                 {pred['arima_forecast']}, {pred['sarima_forecast']});
                """
                conn.execute(insert_sql)
                print(f"Saved forecast for {pred['ticker']}")
    except Exception as e:
        print(f"Error saving forecasts: {e}")
        raise

def main():
    """Main function to run the forecasting pipeline."""
    print("Starting forecasting process")
    
    conn = connect_to_motherduck()
    if not conn:
        return
    
    try:
        # Fetch and prepare data
        df = fetch_stock_data(conn)
        print(f"Fetched {len(df)} rows of data")  # Debug print
        
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
        print("Forecasting completed successfully")
        
    except Exception as e:
        print(f"Error during forecast processing: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()