import os
import logging
from flask import Flask, jsonify
import pandas as pd
from duckdb import connect
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.statespace.sarimax import SARIMAX
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

def connect_to_motherduck():
    """Connect to MotherDuck database"""
    try:
        token = os.environ.get('MOTHERDUCK_TOKEN')
        if not token:
            raise ValueError("MOTHERDUCK_TOKEN environment variable is not set")
        
        logger.info("Attempting to connect to MotherDuck")
        conn = connect(f"md:?token={token}")
        logger.info("Successfully connected to MotherDuck")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to MotherDuck: {str(e)}")
        raise

@app.route('/', methods=['GET'])
def home():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "message": "Service is running"}), 200

@app.route('/train', methods=['POST'])
def train_models():
    try:
        logger.info("Starting model training process")
        conn = connect_to_motherduck()
        db_schema = os.environ.get('DB_SCHEMA', 'default')
        
        logger.info(f"Fetching data from schema: {db_schema}")
        df = conn.sql(f"SELECT * FROM {db_schema}.y_finance").fetchdf()
        
        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date')
        df = df.sort_values(by='date')
        
        predictions_df = pd.DataFrame(columns=['ticker', 'date', 'arima_forecast', 'sarima_forecast'])
        
        for ticker in df['ticker'].unique():
            logger.info(f"Processing ticker: {ticker}")
            stock_df = df[df['ticker'] == ticker].last("2Y")
            
            model_arima = ARIMA(stock_df['close'], order=(5, 1, 0))
            arima_result = model_arima.fit()
            forecast_arima = arima_result.forecast(steps=1)
            
            model_sarima = SARIMAX(stock_df['close'], 
                                 order=(1, 1, 1), 
                                 seasonal_order=(1, 1, 1, 12))
            sarima_result = model_sarima.fit()
            forecast_sarima = sarima_result.forecast(steps=1)
            
            next_day = stock_df.index[-1] + pd.Timedelta(days=1)
            
            temp_df = pd.DataFrame({
                'ticker': [ticker],
                'date': [next_day],
                'arima_forecast': [forecast_arima.iloc[0]],
                'sarima_forecast': [forecast_sarima.iloc[0]]
            })
            
            predictions_df = pd.concat([predictions_df, temp_df], 
                                     ignore_index=True)
        
        logger.info("Storing predictions in database")
        for _, row in predictions_df.iterrows():
            insert_sql = f"""
            INSERT INTO {db_schema}.stock_forecasts 
            (ticker, date, arima_forecast, sarima_forecast)
            VALUES 
            ('{row['ticker']}', '{row['date']}', 
             {row['arima_forecast']}, {row['sarima_forecast']});
            """
            conn.execute(insert_sql)
        
        conn.close()
        logger.info("Training process completed successfully")
        return jsonify({
            "status": "success", 
            "message": "Models trained and predictions stored"
        }), 200
    
    except Exception as e:
        logger.error(f"Error during training: {str(e)}")
        return jsonify({
            "status": "error", 
            "message": str(e)
        }), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
