

import yfinance as yf
from google.cloud import secretmanager
import duckdb
import datetime
import uuid
import functions_framework  # Import the Cloud Functions framework

# settings
project_id = 'ba882-team9'
secret_id = 'mother_duck'
version_id = 'latest'

# db setup
db = 'ba882_team9'
schema = "stage"  # The new schema for y_finance data
db_schema = f"{db}.{schema}"

# Define the companies (ticker symbols)
cloud_companies = ["AMZN", "MSFT"]
chip_companies = ["INTC"]
# Combine cloud and chip companies
companies = cloud_companies + chip_companies

# Function to fetch stock data using yfinance
def get_yfinance_data(ticker):
    """Fetch stock data for the given ticker using yfinance."""
    try:
        stock = yf.Ticker(ticker)
        
        # Calculate the start and end dates for the past 5 years
        end_date = datetime.date.today()
        start_date = end_date - datetime.timedelta(days=5*365)

        history = stock.history(start=start_date, end=end_date)  # Get historical stock data for the past 5 years
        print(f"Fetched history for {ticker}: {history.head()}")
        return history
    except Exception as e:
        print(f"Error fetching Yahoo Finance data for {ticker}: {e}")
        return None

def insert_yfinance_data_to_db(md, ticker, data):
    """Insert the yfinance data into MotherDuck for the given ticker."""
    try:
        for date, row in data.iterrows():
            insert_sql = f"""
                INSERT INTO {db_schema}.y_finance (ticker, time, close, volume) 
                VALUES ('{ticker}', '{date}', {row['Close']}, {row['Volume']})
            """
            print(f"Executing SQL: {insert_sql}")  # Log the SQL being executed
            md.sql(insert_sql)
            print(f"Inserted data for {ticker} on {date}")
    except Exception as e:
        print(f"Error inserting data for {ticker}: {e}")



# The main task function triggered by an HTTP request
@functions_framework.http
def task(request):
    """HTTP Cloud Function to fetch yfinance data and insert it into MotherDuck."""
    try:
        # Instantiate the Secret Manager service
        sm = secretmanager.SecretManagerServiceClient()

        # Build the resource name of the secret version
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

        # Access the secret version
        response = sm.access_secret_version(request={"name": name})
        md_token = response.payload.data.decode("UTF-8")

        # Initiate the MotherDuck connection through an access token
        md = duckdb.connect(f'md:?motherduck_token={md_token}')

        # Fetch data for each company and insert it into MotherDuck
        for company in companies:
            stock_data = get_yfinance_data(company)
            if stock_data is not None:
                insert_yfinance_data_to_db(md, company, stock_data)

        return {"message": "YFinance data successfully inserted into MotherDuck"}, 200
    except Exception as e:
        print(f"Error occurred: {e}")
        return {"error": str(e)}, 500


