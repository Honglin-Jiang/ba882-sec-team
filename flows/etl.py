# The ETL job orchestrator

# imports
import requests
import json
from prefect import flow, task
from datetime import timedelta

# Slack Webhook URL
slack_webhook_url = "https://hooks.slack.com/services/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"

# Helper function - send Slack notification
def send_slack_alert(message: str):
    payload = {"text": message}
    try:
        response = requests.post(slack_webhook_url, json=payload)
        response.raise_for_status()
        print("Slack alert sent successfully")
    except requests.exceptions.RequestException as e:
        print(f"Failed to send Slack alert: {e}")

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

# @task(retries=2)
# def extract_yfinance_3to6yr():
#     """Extract 3 to 6 years ago stock data from yfinance"""
#     url = "https://us-central1-ba882-team9.cloudfunctions.net/extract-yfinance-3to6yr"
#     resp = invoke_gcf(url, payload={})
#     return resp

# @task(retries=2)
# def extract_yfinance():
#     """Extract 3 years ago to now stock data from yfinance"""
#     url = "https://us-central1-ba882-team9.cloudfunctions.net/extract-yfinance"
#     resp = invoke_gcf(url, payload={})
#     return resp

@task(retries=2)
def extract_yfinance_sp100_3mo():
    """Extract 3 months ago to now S&P 100 stock data from yfinance"""
    url = "https://us-central1-ba882-team9.cloudfunctions.net/extract-yfinance-100companies-3mo"
    resp = invoke_gcf(url, payload={})
    send_slack_alert("S&P 100 stock data extraction (3 months ago) completed successfully.")
    return resp

@task(retries=2)
def extract_yfinance_sp100_3_6mo():
    """Extract 6 months ago to 3 months ago S&P 100 stock data from yfinance"""
    url = "https://us-central1-ba882-team9.cloudfunctions.net/extract-yfinance-100companies-3-6mo"
    resp = invoke_gcf(url, payload={})
    send_slack_alert("S&P 100 stock data extraction (3-6 months ago) completed successfully.")
    return resp

@task(retries=2)
def extract_yfinance_sp100_6_9mo():
    """Extract 9 months ago to 6 months ago S&P 100 stock data from yfinance"""
    url = "https://us-central1-ba882-team9.cloudfunctions.net/extract-yfinance-100companies-6-9mo"
    resp = invoke_gcf(url, payload={})
    send_slack_alert("S&P 100 stock data extraction (6-9 months ago) completed successfully.")
    return resp

@task(retries=2)
def extract_yfinance_sp100_9_12mo():
    """Extract 12 months ago to 9 months ago S&P 100 stock data from yfinance"""
    url = "https://us-central1-ba882-team9.cloudfunctions.net/extract-yfinance-100companies-9-12mo"
    resp = invoke_gcf(url, payload={})
    send_slack_alert("S&P 100 stock data extraction (9-12 months ago) completed successfully.")
    return resp

@task(retries=3)
def extract_yfinance_sp100_12_15mo():
    """Extract 15 months ago to 12 months ago S&P 100 stock data from yfinance"""
    url = "https://us-central1-ba882-team9.cloudfunctions.net/extract-yfinance-100companies-12-15mo"
    resp = invoke_gcf(url, payload={})
    send_slack_alert("S&P 100 stock data extraction (12-15 months ago) completed successfully.")
    return resp

@task(retries=3)
def extract_yfinance_sp100_15_18mo():
    """Extract 18 months ago to 15 months ago S&P 100 stock data from yfinance"""
    url = "https://us-central1-ba882-team9.cloudfunctions.net/extract-yfinance-100companies-15-18mo"
    resp = invoke_gcf(url, payload={})
    send_slack_alert("S&P 100 stock data extraction (15-18 months ago) completed successfully.")
    return resp

@task(retries=3)
def extract_yfinance_sp100_18_21mo():
    """Extract 21 months ago to 18 months ago S&P 100 stock data from yfinance"""
    url = "https://us-central1-ba882-team9.cloudfunctions.net/extract-yfinance-100companies-18-21mo"
    resp = invoke_gcf(url, payload={})
    send_slack_alert("S&P 100 stock data extraction (18-21 months ago) completed successfully.")
    return resp

@task(retries=3)
def extract_yfinance_sp100_21_24mo():
    """Extract 24 months ago to 21 months ago S&P 100 stock data from yfinance"""
    url = "https://us-central1-ba882-team9.cloudfunctions.net/extract-yfinance-100companies-21-24mo"
    resp = invoke_gcf(url, payload={})
    send_slack_alert("S&P 100 stock data extraction (21-24 months ago) completed successfully.")
    return resp

@task(retries=15)
def extract_yfinance_sp100_24_30mo():
    """Extract 30 months ago to 24 months ago S&P 100 stock data from yfinance"""
    url = "https://us-central1-ba882-team9.cloudfunctions.net/extract-yfinance-100companies-24-30mo"
    resp = invoke_gcf(url, payload={})
    send_slack_alert("S&P 100 stock data extraction (24-30 months ago) completed successfully.")
    return resp

@task(retries=30)
def extract_yfinance_sp100_30_60mo():
    """Extract 60 months ago to 30 months ago S&P 100 stock data from yfinance"""
    url = "https://us-central1-ba882-team9.cloudfunctions.net/extract-yfinance-100companies-30-60mo"
    resp = invoke_gcf(url, payload={})
    send_slack_alert("S&P 100 stock data extraction (30-60 months ago) completed successfully.")
    return resp

@task(retries=5)
def transform_load_yfinance(payload):
    """Process the RSS feed JSON into parquet on GCS"""
    url = "https://us-central1-ba882-team9.cloudfunctions.net/transform-yfinance"
    resp = invoke_gcf(url, payload=payload)
    send_slack_alert("Data transform & load completed successfully.")
    return resp


# Prefect Flow
@flow(name="ba882-team9-etl-flow", log_prints=True)
def etl_flow():
    """The ETL flow which orchestrates Cloud Functions"""

    try:
        result = schema_setup()
        print("The schema setup completed")
        
        # extract_mda_result = extract_mda()
        # print("The mda were extracted onto motherdb")
        # print(f"{extract_mda_result}")

        # extract_yfinance_3to6yr_result = extract_yfinance_3to6yr()
        # print("The yfinance data 3 to 6 years ago were extracted into motherduck db")
        # print(f"{extract_yfinance_3to6yr_result}")

        # extract_yfinance_result = extract_yfinance()
        # print("The yfinance data 3 years ago until now were extracted into motherduck db")
        # print(f"{extract_yfinance_result}")

        # extract_yfinance_sp100_3mo_result = extract_yfinance_sp100_3mo()
        # print("The yfinance data 3 months ago were extracted into motherduck db")
        # print(f"{extract_yfinance_sp100_3mo_result}")

        # extract_yfinance_sp100_3_6mo_result = extract_yfinance_sp100_3_6mo()
        # print("The yfinance data for range 3-6 months ago were extracted into motherduck db")
        # print(f"{extract_yfinance_sp100_3_6mo_result}")

        # extract_yfinance_sp100_6_9mo_result = extract_yfinance_sp100_6_9mo()
        # print("The yfinance data for range 6-9 months ago were extracted into motherduck db")
        # print(f"{extract_yfinance_sp100_6_9mo_result}")

        # extract_yfinance_sp100_9_12mo_result = extract_yfinance_sp100_9_12mo()
        # print("The yfinance data for range 9-12 months ago were extracted into motherduck db")
        # print(f"{extract_yfinance_sp100_9_12mo_result}")

        # extract_yfinance_sp100_12_15mo_result = extract_yfinance_sp100_12_15mo()
        # print("The yfinance data for range 12-15 months ago were extracted into motherduck db")
        # print(f"{extract_yfinance_sp100_12_15mo_result}")

        # extract_yfinance_sp100_15_18mo_result = extract_yfinance_sp100_15_18mo()
        # print("The yfinance data for range 15-18 months ago were extracted into motherduck db")
        # print(f"{extract_yfinance_sp100_15_18mo_result}")

        # extract_yfinance_sp100_18_21mo_result = extract_yfinance_sp100_18_21mo()
        # print("The yfinance data for range 18-21 months ago were extracted into motherduck db")
        # print(f"{extract_yfinance_sp100_18_21mo_result}")

        # extract_yfinance_sp100_21_24mo_result = extract_yfinance_sp100_21_24mo()
        # print("The yfinance data for range 21-24 months ago were extracted into motherduck db")
        # print(f"{extract_yfinance_sp100_21_24mo_result}")
        
        extract_yfinance_sp100_24_30mo_result = extract_yfinance_sp100_24_30mo()
        print("The yfinance data for range 24-30 months ago were extracted into motherduck db")
        print(f"{extract_yfinance_sp100_24_30mo_result}")

        extract_yfinance_sp100_30_60mo_result = extract_yfinance_sp100_30_60mo()
        print("The yfinance data for range 30-60 months ago were extracted into motherduck db")
        print(f"{extract_yfinance_sp100_30_60mo_result}")

        # transform_load_yfinance_result = transform_load_yfinance(extract_yfinance_result)
        # print("The yfinance data were transformed and stored into motherduck db")
        # print(f"{transform_load_yfinance_result}")

        send_slack_alert("The entire pipeline has successfully completed!")

    except Exception as e:
        # Send a failure alert in case of any exception
        send_slack_alert(f"ETL Flow encountered an error: {str(e)}")
        raise

# the job
if __name__ == "__main__":
    etl_flow()

