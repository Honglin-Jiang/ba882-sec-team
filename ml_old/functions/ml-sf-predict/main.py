import os
import logging
from flask import Flask, jsonify, request
from duckdb import connect
import pandas as pd
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

def connect_to_motherduck():
    """Connect to MotherDuck database"""
    try:
        token = os.environ.get('mother_duck')
        if not token:
            raise ValueError("mother_duckenvironment variable is not set")
        
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

@app.route('/predict', methods=['POST'])
def make_predictions():
    try:
        logger.info("Starting prediction process")
        conn = connect_to_motherduck()
        db_schema = os.environ.get('DB_SCHEMA', 'default')
        
        # Get parameters from request
        data = request.get_json()
        ticker = data.get('ticker')
        
        if not ticker:
            return jsonify({"status": "error", "message": "Ticker is required"}), 400

        logger.info(f"Getting predictions for ticker: {ticker}")
        
        # Fetch the latest predictions for the specified ticker
        query = f"""
        SELECT * FROM {db_schema}.stock_forecasts 
        WHERE ticker = '{ticker}' 
        ORDER BY date DESC 
        LIMIT 1
        """
        
        result = conn.sql(query).fetchdf()
        
        if result.empty:
            return jsonify({
                "status": "error",
                "message": f"No predictions found for ticker {ticker}"
            }), 404
        
        prediction = {
            "ticker": ticker,
            "date": result['date'].iloc[0].strftime('%Y-%m-%d'),
            "arima_forecast": float(result['arima_forecast'].iloc[0]),
            "sarima_forecast": float(result['sarima_forecast'].iloc[0])
        }
        
        conn.close()
        logger.info(f"Successfully retrieved predictions for {ticker}")
        return jsonify({
            "status": "success", 
            "prediction": prediction
        }), 200
    
    except Exception as e:
        logger.error(f"Error during prediction: {str(e)}")
        return jsonify({
            "status": "error", 
            "message": str(e)
        }), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
