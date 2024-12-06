from datetime import datetime, timedelta
import yfinance as yf
from google.cloud import secretmanager
import duckdb
import functions_framework

# Settings
project_id = 'ba882-team9'
secret_id = 'mother_duck'
version_id = 'latest'

# DB Setup
db = 'ba882_team9'
schema = "stage"
db_schema = f"{db}.{schema}"

# Define the companies (tickers)
cloud_companies = ["AMZN", "MSFT", "GOOG", "BABA", "CRM"]
chip_companies = ["INTC", "NVDA", "TSM", "AMD"]
companies = cloud_companies + chip_companies

# Define the time range (last 7 years)
today = datetime.today()
three_years_ago = today - timedelta(days=7 * 365)

# Function to fetch Yahoo Finance news for a specific ticker
def get_yfinance_news(ticker):
    """Fetch news for the given ticker using yfinance and filter for the last 3 years."""
    try:
        stock = yf.Ticker(ticker)
        news = stock.news

        # Check if news is empty
        if not news:
            print(f"No news found for {ticker}")
            return None

        # Filter news within the last 7 years
        filtered_news = [
            article for article in news
            if datetime.fromtimestamp(article["providerPublishTime"]) >= three_years_ago
        ]

        print(f"Fetched {len(filtered_news)} news articles for {ticker} in the last 3 years.")
        return filtered_news
    except Exception as e:
        print(f"Error fetching news for {ticker}: {e}")
        return None

def insert_yfinance_news_to_db(md, ticker, news):
    """Insert the filtered news into MotherDuck for the given ticker."""
    try:
        for article in news:
            # Extract fields
            news_time = datetime.fromtimestamp(article["providerPublishTime"])
            title = article.get("title", "").replace("'", "''")  # Escape single quotes
            url = article.get("link", "")
            source = article.get("publisher", "")

            # Check if record exists
            check_sql = f"""
                SELECT COUNT(*) FROM {db_schema}.y_finance_news
                WHERE ticker = '{ticker}' AND news_time = '{news_time}' AND title = '{title}';
            """
            result = md.sql(check_sql).fetchone()[0]

            if result == 0:  # If no record exists, insert data
                insert_sql = f"""
                    INSERT INTO {db_schema}.y_finance_news (ticker, news_time, title, url, source)
                    VALUES ('{ticker}', '{news_time}', '{title}', '{url}', '{source}');
                """
                md.sql(insert_sql)
                print(f"Inserted news for {ticker} on {news_time}: {title}")
            else:
                print(f"News article already exists for {ticker} on {news_time}: {title}")
    except Exception as e:
        print(f"Error inserting news for {ticker}: {e}")

# Main task function triggered by an HTTP request
@functions_framework.http
def task(request):
    """HTTP Cloud Function to fetch Yahoo Finance news and insert it into MotherDuck."""
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

        # Fetch news for each company and insert into MotherDuck
        for company in companies:
            news_data = get_yfinance_news(company)
            if news_data:
                insert_yfinance_news_to_db(md, company, news_data)

        return {"message": "YFinance news successfully inserted into MotherDuck"}, 200
    except Exception as e:
        print(f"Error occurred: {e}")
        return {"error": str(e)}, 500
