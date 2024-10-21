# The ETL job orchestrator

# imports
import requests
import json
from prefect import flow, task

# helper function - generic invoker
def invoke_gcf(url:str, payload:dict):
    response = requests.post(url, json=payload)
    response.raise_for_status()
    return response.json()

@task(retries=2)
def schema_setup():
    """Setup the stage schema"""
    url = "https://us-central1-ba882-team9.cloudfunctions.net/schema-setup"
    resp = invoke_gcf(url, payload={})
    return resp

@task(retries=2)
def extract_mda():
    """Extract MD&A from sec api"""
    url = "https://us-central1-ba882-team9.cloudfunctions.net/extract-mda"
    resp = invoke_gcf(url, payload={})
    return resp

@task(retries=2)
def extract_yfinance():
    """Extract stock data from yfinance"""
    url = "https://us-central1-ba882-team9.cloudfunctions.net/extract-yfinance"
    resp = invoke_gcf(url, payload={})
    return resp

@task(retries=2)
def parse_yfinance(payload):
    """Process the y_finance feed JSON into parquet on GCS"""
    url = "https://us-central1-ba882-team9.cloudfunctions.net/parse-api"
    resp = invoke_gcf(url, payload=payload)
    return resp

# @task(retries=2)
# def transform(payload):
#     """Process the RSS feed JSON into parquet on GCS"""
#     url = "https://us-central1-btibert-ba882-fall24.cloudfunctions.net/dev-parse-rss"
#     resp = invoke_gcf(url, payload=payload)
#     return resp

@task(retries=2)
def load_api_yfinance(payload):
    """Load the tables into the raw schema, ingest new records into stage tables"""
    url = "https://us-central1-ba882-team9.cloudfunctions.net/load-api"
    resp = invoke_gcf(url, payload=payload)
    return resp

# @task(retries=2)
# def load(payload):
#     """Load the tables into the raw schema, ingest new records into stage tables"""
#     url = "https://us-central1-btibert-ba882-fall24.cloudfunctions.net/dev-load-rss"
#     resp = invoke_gcf(url, payload=payload)
#     return resp

# Prefect Flow
@flow(name="ba882-team9-etl-flow", log_prints=True)
def etl_flow():
    """The ETL flow which orchestrates Cloud Functions"""

    result = schema_setup()
    print("The schema setup completed")
    
    # extract_mda_result = extract_mda()
    # print("The mda were extracted onto motherdb")
    # print(f"{extract_mda_result}")

    extract_yfinance_result = extract_yfinance()
    print("The market data were extracted onto motherdb")
    print(f"{extract_yfinance_result}")
    
    # parse_yfinance_result = parse_yfinance(extract_yfinance_result)
    # print("The parsing of the feeds into tables completed")
    # print(f"{parse_yfinance_result}")

    load_yfinance_result = load_api_yfinance(extract_yfinance_result)
    print("The y_finance data were loaded into the raw schema and changes added to stage")

# the job
if __name__ == "__main__":
    etl_flow()

