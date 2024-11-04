import yfinance as yf
from google.cloud import secretmanager
import duckdb
import datetime
import functions_framework
import pytz
from datetime import datetime, timedelta

# settings
project_id = 'ba882-team9'
secret_id = 'mother_duck'
version_id = 'latest'

# db setup
db = 'ba882_team9'
schema = "stage"
db_schema = f"{db}.{schema}"

# Define the companies (ticker)
# Information_Technology = ['AAPL', 'ACN', 'ADBE', 'AMD', 'AVGO', 'CRM', 'CSCO', 'IBM','INTC', 'INTU', 'MA', 'MSFT', 'NVDA', 'ORCL', 'PYPL', 'QCOM', 'TXN', 'V']  #18
# Health_Care = ['ABBV', 'ABT', 'AMGN', 'CVS', 'DHR', 'GILD', 'JNJ', 'LLY', 'MDT', 'MRK','PFE', 'TMO', 'UNH'] #13
# Financials = ['AIG', 'AXP', 'BAC', 'BK', 'BLK', 'BRK.B', 'C', 'COF', 'GS', 'JPM','MET', 'MS', 'SCHW', 'USB', 'WFC'] #15
# Consumer_Discretionary = ['AMZN', 'BKNG', 'F', 'GM', 'HD', 'LOW', 'MCD', 'NKE', 'SBUX', 'TGT', 'TSLA']  #11
# Communication_Services = ['CHTR', 'CMCSA', 'DIS', 'GOOG', 'GOOGL', 'META', 'NFLX', 'T', 'TMUS', 'VZ']   #10
# Consumer_Staples = ['CL', 'COST', 'KHC', 'KO', 'MDLZ', 'MO', 'PEP', 'PG', 'PM', 'WMT']  #10
# Industrials = ['BA', 'CAT', 'DE', 'EMR', 'FDX', 'GD', 'GE', 'HON', 'LMT', 'MMM','RTX', 'UNP', 'UPS']    #13
# Utilities = ['DUK', 'NEE', 'SO']
# Energy = ['COP', 'CVX', 'XOM']
# Real_Estate = ['AMT', 'SPG']
# Materials = ['DOW', 'LIN']

# companies = Information_Technology + Health_Care + Financials + Consumer_Discretionary + Communication_Services + Consumer_Staples + Industrials + Utilities + Energy + Real_Estate + Materials

cloud_companies = ["AMZN", "MSFT", "GOOG", "BABA", "CRM"]
chip_companies = ["INTC", "NVDA", "TSM", "AMD"]
companies = cloud_companies + chip_companies

# Function to fetch stock data using yfinance
def get_yfinance_data(ticker):
    """Fetch stock data for the given ticker using yfinance for 5 year ago and filter to 30-36 months range."""
    try:
        # Fetch the last 5 year of data and filter to 30-36 months ago
        end_date = datetime.now() - timedelta(days=30 * 30)
        start_date = datetime.now() - timedelta(days=30 * 36)
        
        # Fetch the last 5 year of data
        stock = yf.Ticker(ticker)
        history = stock.history(period="5y")

        # Convert start_date and end_date to the same timezone as history.index
        start_date = start_date.replace(tzinfo=pytz.timezone('America/New_York'))
        end_date = end_date.replace(tzinfo=pytz.timezone('America/New_York'))
        history = history[(history.index >= start_date) & (history.index <= end_date)]
        
        print(f"Fetched {len(history)} records for {ticker}. Data preview: {history.head()}")
        
        # Check if data is empty
        if history.empty:
            print(f"No data found for {ticker} in the specified date range")
            return None

        return history
    except Exception as e:
        print(f"Error fetching Yahoo Finance data for {ticker}: {e}")
        return None

def insert_yfinance_data_to_db(md, ticker, data):
    """Insert the yfinance data into MotherDuck for the given ticker."""
    try:
        for date, row in data.iterrows():
            # Check if there are the same records
            check_sql = f"""
                SELECT COUNT(*) FROM {db_schema}.y_finance 
                WHERE ticker = '{ticker}' AND time = '{date}';
            """
            result = md.sql(check_sql).fetchone()[0]

            if result == 0:  # If no, then insert data
                insert_sql = f"""
                    INSERT INTO {db_schema}.y_finance (ticker, time, close, volume) 
                    VALUES ('{ticker}', '{date}', {row['Close']}, {row['Volume']});
                """
                md.sql(insert_sql)
                print(f"Inserted data for {ticker} on {date}")
            else:
                print(f"Record already exists for {ticker} on {date}")
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

        return {"message": "YFinance 9 companies stock data for 30 to 36 months ago successfully inserted into MotherDuck"}, 200
    except Exception as e:
        print(f"Error occurred: {e}")
        return {"error": str(e)}, 500
