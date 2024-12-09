# The ETL job orchestrator

# imports
import requests
import json
from prefect import flow, task
from datetime import timedelta
from google.cloud import secretmanager

# Helper function - access any secret from Secret Manager
def get_secret(secret_id, project_id="ba882-team9"):
    # Create a Secret Manager client
    sm = secretmanager.SecretManagerServiceClient()

    # Construct the resource path for the secret
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"

    # Access the secret
    response = sm.access_secret_version(request={"name": name})

    # Return the secret value
    return response.payload.data.decode("UTF-8")


# Helper function - send Slack notification
def send_slack_alert(message: str):
    # Retrieve the Slack Webhook URL from Secret Manager
    slack_webhook_url = get_secret(secret_id="SLACK_WEBHOOK_URL")
    payload = {"text": message}
    try:
        response = requests.post(slack_webhook_url, json=payload)
        response.raise_for_status()
        print("Slack alert sent successfully")
    except requests.exceptions.RequestException as e:
        print(f"Failed to send Slack alert: {e}")

# Retrieve a Slack Webhook URL
slack_webhook_url = get_secret(secret_id="SLACK_WEBHOOK_URL")

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
    send_slack_alert("Schema setup completed successfully.")
    return resp

@task(retries=2)
def extract_mda():
    """Extract MD&A from sec api"""
    url = "https://us-central1-ba882-team9.cloudfunctions.net/extract-mda"
    resp = invoke_gcf(url, payload={})
    send_slack_alert("MD&A extraction completed successfully.")
    return resp

@task(retries=5)
def extract_yfinance_9companies_7d():
    """Extract 9 companies 7 days ago stock data from yfinance"""
    url = "https://us-central1-ba882-team9.cloudfunctions.net/extract-yfinance-9companies-7d"
    resp = invoke_gcf(url, payload={})
    send_slack_alert("9 companies 7 days ago stock data extraction completed successfully.")
    return resp

@task(retries=5)
def transform_load_yfinance_7d():
    """Trigger the Cloud Function to process and load the last 7 days of YFinance data."""
    url = "https://us-central1-ba882-team9.cloudfunctions.net/transform-load-yfinance-7d"
    # Invoke the Google Cloud Function without additional payload
    resp = invoke_gcf(url, payload={})
    # Send a Slack alert upon successful transformation and load
    send_slack_alert("YFinance data transform & load for the last 7 days completed successfully.")
    return resp

@task(retries=5)
def extract_mda_9companies_7y():
    """Extract 9 companies 7 years ago MD&A data"""
    url = "https://us-central1-ba882-team9.cloudfunctions.net/extract-mda-v2"
    resp = invoke_gcf(url, payload={})
    send_slack_alert("9 companies 7 years ago MD&A data extraction completed successfully.")
    return resp

@task(retries=5)
def extract_yfinance_news_9companies_sameday():
    """Extract 9 companies same day yfinance news data"""
    url = "https://us-central1-ba882-team9.cloudfunctions.net/extract-yfinance-news-9companies-sameday"
    resp = invoke_gcf(url, payload={})
    send_slack_alert("9 companies same day yfinance news data extraction completed successfully.")
    return resp

# Prefect Flow
@flow(name="ba882-team9-etl-flow", log_prints=True)
def etl_flow():
    """The ETL flow which orchestrates Cloud Functions"""
    try:
        send_slack_alert("******************************** New run initiated! ********************************")

        result = schema_setup()
        print("The schema setup completed")
        
        extract_yfinance_news_9companies_sameday_result = extract_yfinance_news_9companies_sameday()
        print("The same day yfinance news data (9 companies) were extracted into MotherDuck DB")
        print(f"{extract_yfinance_news_9companies_sameday_result}")

        extract_yfinance_9companies_7d_result = extract_yfinance_9companies_7d()
        print("The yfinance data (9 companies) for range 7 days ago were extracted into MotherDuck DB")
        print(f"{extract_yfinance_9companies_7d_result}")

        transform_load_yfinance_result = transform_load_yfinance_7d()
        print("The YFinance data for the last 7 days (9 companies) were transformed and stored into MotherDuck DB")
        print(f"{transform_load_yfinance_result}")

        extract_mda_9companies_7y_result = extract_mda_9companies_7y()
        print("The MD&A data for the last 7 years (9 companies) were extracted into MotherDuck DB")
        print(f"{extract_mda_9companies_7y_result}")
    
        send_slack_alert("******************************** The entire pipeline has successfully completed! ********************************")

    except Exception as e:
        # Send a failure alert in case of any exception
        send_slack_alert(f"ETL Flow encountered an error: {str(e)}")
        send_slack_alert("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! The pipeline ran into an error. !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        raise

# the job
if __name__ == "__main__":
    etl_flow()

