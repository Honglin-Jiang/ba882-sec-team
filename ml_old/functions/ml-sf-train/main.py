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

def connect_to_motherduck(read_only=False):
    """Connect to MotherDuck database"""
    try:
        token = os.environ.get('mother_duck')
        if not token:
            raise ValueError("mother_duck environment variable is not set")
        
        logger.info(f"Attempting to connect to MotherDuck (read_only={read_only})")
        if read_only:
            conn = connect(f"md:?token={token}&read_only=true")
        else:
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
    read_conn = None
    write_conn = None
    
    try:
        logger.info("Starting model training process")
        
        # First, read data using read-only connection
        read_conn = connect_to_motherduck(read_only=True)
        logger.info("Fetching data from transformed schema")
        
        df = read_conn.sql("SELECT * FROM ba882_team9.transformed.y_finance").fetchdf()
        read_conn.close()
        read_conn = None
        
        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date')
        df = df.sort_values(by='date')
        
        predictions_df = pd.DataFrame(columns=['ticker', 'date', 'arima_forecast', 'sarima_forecast'])
        
        for ticker in df['ticker'].unique():
            logger.info(f"Processing ticker: {ticker}")
            stock_df = df[df['ticker'] == ticker].last("2Y")
            
            if len(stock_df) < 2:
                logger.warning(f"Not enough data for ticker {ticker}")
                continue
                
            try:
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
                logger.info(f"Generated predictions for {ticker}")
            except Exception as e:
                logger.error(f"Error processing ticker {ticker}: {str(e)}")
                continue
        
        if len(predictions_df) > 0:
            # Now connect for writing
            write_conn = connect_to_motherduck(read_only=False)
            
            logger.info("Storing predictions in transformed schema")
            
            # Clear existing predictions
            write_conn.execute("DELETE FROM ba882_team9.transformed.stock_forecasts")
            logger.info("Cleared existing predictions")
            
            # Insert new predictions
            for _, row in predictions_df.iterrows():
                insert_sql = f"""
                INSERT INTO ba882_team9.transformed.stock_forecasts 
                (ticker, date, arima_forecast, sarima_forecast)
                VALUES 
                ('{row['ticker']}', '{row['date']}', 
                 {row['arima_forecast']}, {row['sarima_forecast']});
                """
                write_conn.execute(insert_sql)
                
            logger.info(f"Successfully stored {len(predictions_df)} predictions")
            write_conn.close()
            write_conn = None
        else:
            logger.warning("No predictions were generated")
        
        return jsonify({
            "status": "success", 
            "message": "Models trained and predictions stored",
            "predictions_count": len(predictions_df)
        }), 200
    
    except Exception as e:
        logger.error(f"Error during training: {str(e)}")
        return jsonify({
            "status": "error", 
            "message": str(e)
        }), 500
    
    finally:
        if read_conn:
            read_conn.close()
        if write_conn:
            write_conn.close()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))