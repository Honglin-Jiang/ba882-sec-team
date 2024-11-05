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

################################################### 100 Companies Code Below ###################################################
'''
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

#Error
@task(retries=15)
def extract_yfinance_sp100_24_30mo():
    """Extract 30 months ago to 24 months ago S&P 100 stock data from yfinance"""
    url = "https://us-central1-ba882-team9.cloudfunctions.net/extract-yfinance-100companies-24-30mo"
    resp = invoke_gcf(url, payload={})
    send_slack_alert("S&P 100 stock data extraction (24-30 months ago) completed successfully.")
    return resp

#Error
@task(retries=30)
def extract_yfinance_sp100_30_60mo():
    """Extract 60 months ago to 30 months ago S&P 100 stock data from yfinance"""
    url = "https://us-central1-ba882-team9.cloudfunctions.net/extract-yfinance-100companies-30-60mo"
    resp = invoke_gcf(url, payload={})
    send_slack_alert("S&P 100 stock data extraction (30-60 months ago) completed successfully.")
    return resp

@task(retries=10)
def extract_yfinance_sp100_30_35mo():
    """Extract 35 months ago to 30 months ago S&P 100 stock data from yfinance"""
    url = "https://us-central1-ba882-team9.cloudfunctions.net/extract-yfinance-100companies-30-35mo"
    resp = invoke_gcf(url, payload={})
    send_slack_alert("S&P 100 stock data extraction (30-35 months ago) completed successfully.")
    return resp
'''
################################################### 100 Companies Code Above ###################################################

@task(retries=5)
def extract_yfinance_9companies_6mo():
    """Extract 6 months 9 companies stock data from yfinance"""
    url = "https://us-central1-ba882-team9.cloudfunctions.net/extract-yfinance-9companies-6mo"
    resp = invoke_gcf(url, payload={})
    send_slack_alert("9 companies 6 months stock data extraction completed successfully.")
    return resp

@task(retries=5)
def extract_yfinance_9companies_6_12mo():
    """Extract 9 companies 6-12 months ago stock data from yfinance"""
    url = "https://us-central1-ba882-team9.cloudfunctions.net/extract-yfinance-9companies-6-12mo"
    resp = invoke_gcf(url, payload={})
    send_slack_alert("9 companies 6-12 months ago stock data extraction completed successfully.")
    return resp

@task(retries=5)
def extract_yfinance_9companies_12_18mo():
    """Extract 9 companies 12-18 months ago stock data from yfinance"""
    url = "https://us-central1-ba882-team9.cloudfunctions.net/extract-yfinance-9companies-12-18mo"
    resp = invoke_gcf(url, payload={})
    send_slack_alert("9 companies 12-18 months ago stock data extraction completed successfully.")
    return resp

@task(retries=5)
def extract_yfinance_9companies_18_24mo():
    """Extract 9 companies 18-24 months ago stock data from yfinance"""
    url = "https://us-central1-ba882-team9.cloudfunctions.net/extract-yfinance-9companies-18-24mo"
    resp = invoke_gcf(url, payload={})
    send_slack_alert("9 companies 18-24 months ago stock data extraction completed successfully.")
    return resp

@task(retries=5)
def extract_yfinance_9companies_24_30mo():
    """Extract 9 companies 24-30 months ago stock data from yfinance"""
    url = "https://us-central1-ba882-team9.cloudfunctions.net/extract-yfinance-9companies-24-30mo"
    resp = invoke_gcf(url, payload={})
    send_slack_alert("9 companies 24-30 months ago stock data extraction completed successfully.")
    return resp

@task(retries=5)
def extract_yfinance_9companies_30_36mo():
    """Extract 9 companies 30-36 months ago stock data from yfinance"""
    url = "https://us-central1-ba882-team9.cloudfunctions.net/extract-yfinance-9companies-30-36mo"
    resp = invoke_gcf(url, payload={})
    send_slack_alert("9 companies 30-36 months ago stock data extraction completed successfully.")
    return resp

@task(retries=5)
def extract_yfinance_9companies_36_42mo():
    """Extract 9 companies 36-42 months ago stock data from yfinance"""
    url = "https://us-central1-ba882-team9.cloudfunctions.net/extract-yfinance-9companies-36-42mo"
    resp = invoke_gcf(url, payload={})
    send_slack_alert("9 companies 36-42 months ago stock data extraction completed successfully.")
    return resp

@task(retries=5)
def extract_yfinance_9companies_42_48mo():
    """Extract 9 companies 42-48 months ago stock data from yfinance"""
    url = "https://us-central1-ba882-team9.cloudfunctions.net/extract-yfinance-9companies-42-48mo"
    resp = invoke_gcf(url, payload={})
    send_slack_alert("9 companies 42-48 months ago stock data extraction completed successfully.")
    return resp

@task(retries=5)
def extract_yfinance_9companies_48_54mo():
    """Extract 9 companies 48-54 months ago stock data from yfinance"""
    url = "https://us-central1-ba882-team9.cloudfunctions.net/extract-yfinance-9companies-48-54mo"
    resp = invoke_gcf(url, payload={})
    send_slack_alert("9 companies 48-54 months ago stock data extraction completed successfully.")
    return resp

@task(retries=5)
def extract_yfinance_9companies_54_60mo():
    """Extract 9 companies 54-60 months ago stock data from yfinance"""
    url = "https://us-central1-ba882-team9.cloudfunctions.net/extract-yfinance-9companies-54-60mo"
    resp = invoke_gcf(url, payload={})
    send_slack_alert("9 companies 54-60 months ago stock data extraction completed successfully.")
    return resp

@task(retries=5)
def extract_yfinance_9companies_60_66mo():
    """Extract 9 companies 60-66 months ago stock data from yfinance"""
    url = "https://us-central1-ba882-team9.cloudfunctions.net/extract-yfinance-9companies-60-66mo"
    resp = invoke_gcf(url, payload={})
    send_slack_alert("9 companies 60-66 months ago stock data extraction completed successfully.")
    return resp

@task(retries=5)
def extract_yfinance_9companies_66_72mo():
    """Extract 9 companies 66-72 months ago stock data from yfinance"""
    url = "https://us-central1-ba882-team9.cloudfunctions.net/extract-yfinance-9companies-66-72mo"
    resp = invoke_gcf(url, payload={})
    send_slack_alert("9 companies 66-72 months ago stock data extraction completed successfully.")
    return resp

@task(retries=5)
def extract_yfinance_9companies_72_78mo():
    """Extract 9 companies 72-78 months ago stock data from yfinance"""
    url = "https://us-central1-ba882-team9.cloudfunctions.net/extract-yfinance-9companies-72-78mo"
    resp = invoke_gcf(url, payload={})
    send_slack_alert("9 companies 72-78 months ago stock data extraction completed successfully.")
    return resp

@task(retries=5)
def extract_yfinance_9companies_78_84mo():
    """Extract 9 companies 78-84 months ago stock data from yfinance"""
    url = "https://us-central1-ba882-team9.cloudfunctions.net/extract-yfinance-9companies-78-84mo"
    resp = invoke_gcf(url, payload={})
    send_slack_alert("9 companies 78-84 months ago stock data extraction completed successfully.")
    return resp

#@task(retries=10)
#def transform_load_yfinance(payload, batch_name):
#    """Process the RSS feed JSON into parquet on GCS and send a customized Slack alert."""
#    url = "https://us-central1-ba882-team9.cloudfunctions.net/transform-load-yfinance"
#    resp = invoke_gcf(url, payload=payload)
#    send_slack_alert(f"Yfinance data ({batch_name}) transform & load completed successfully.")
#    return resp

@task(retries=10)
def transform_load_yfinance(offset, batch_size):
    """Calls the Cloud Function to process a specific batch of data."""
    url = "https://us-central1-ba882-team9.cloudfunctions.net/transform-load-yfinance"
    params = {"offset": offset, "batch_size": batch_size}
    response = requests.get(url, params=params)
    result = response.json()
    print(result["message"])
    return result

# Prefect Flow
@flow(name="ba882-team9-etl-flow", log_prints=True)
def etl_flow():
    """The ETL flow which orchestrates Cloud Functions"""

    try:
        send_slack_alert("******************************** New run initiated! ********************************")

        result = schema_setup()
        print("The schema setup completed")
        
        # extract_mda_result = extract_mda()
        # print("The mda were extracted onto motherdb")
        # print(f"{extract_mda_result}")

################################################### 100 Companies Code Below ###################################################
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
        
        # Still error
        #extract_yfinance_sp100_24_30mo_result = extract_yfinance_sp100_24_30mo()
        #print("The yfinance data for range 24-30 months ago were extracted into motherduck db")
        #print(f"{extract_yfinance_sp100_24_30mo_result}")

        # Still error
        # extract_yfinance_sp100_30_60mo_result = extract_yfinance_sp100_30_60mo()
        # print("The yfinance data for range 30-60 months ago were extracted into motherduck db")
        # print(f"{extract_yfinance_sp100_30_60mo_result}")

        # extract_yfinance_sp100_30_35mo_result = extract_yfinance_sp100_30_35mo()
        # print("The yfinance data for range 30-35 months ago were extracted into motherduck db")
        # print(f"{extract_yfinance_sp100_30_35mo_result}")
################################################### 100 Companies Code Above ###################################################



        extract_yfinance_9companies_6mo_result = extract_yfinance_9companies_6mo()
        print("The yfinance data (9 companies) for range 6 months ago range were extracted into motherduck db")
        print(f"{extract_yfinance_9companies_6mo_result}")

        extract_yfinance_9companies_6_12mo_result = extract_yfinance_9companies_6_12mo()
        print("The yfinance data (9 companies) for range 6-12 months ago range were extracted into motherduck db")
        print(f"{extract_yfinance_9companies_6_12mo_result}")

        extract_yfinance_9companies_12_18mo_result = extract_yfinance_9companies_12_18mo()
        print("The yfinance data (9 companies) for range 12-18 months ago range were extracted into motherduck db")
        print(f"{extract_yfinance_9companies_12_18mo_result}")

        extract_yfinance_9companies_18_24mo_result = extract_yfinance_9companies_18_24mo()
        print("The yfinance data (9 companies) for range 18-24 months ago range were extracted into motherduck db")
        print(f"{extract_yfinance_9companies_18_24mo_result}")

        extract_yfinance_9companies_24_30mo_result = extract_yfinance_9companies_24_30mo()
        print("The yfinance data (9 companies) for range 24-30 months ago range were extracted into motherduck db")
        print(f"{extract_yfinance_9companies_24_30mo_result}")

        extract_yfinance_9companies_30_36mo_result = extract_yfinance_9companies_30_36mo()
        print("The yfinance data (9 companies) for range 30-36 months ago range were extracted into motherduck db")
        print(f"{extract_yfinance_9companies_30_36mo_result}")

        extract_yfinance_9companies_36_42mo_result = extract_yfinance_9companies_36_42mo()
        print("The yfinance data (9 companies) for range 36-42 months ago range were extracted into motherduck db")
        print(f"{extract_yfinance_9companies_36_42mo_result}")

        extract_yfinance_9companies_42_48mo_result = extract_yfinance_9companies_42_48mo()
        print("The yfinance data (9 companies) for range 42-48 months ago range were extracted into motherduck db")
        print(f"{extract_yfinance_9companies_42_48mo_result}")

        extract_yfinance_9companies_48_54mo_result = extract_yfinance_9companies_48_54mo()
        print("The yfinance data (9 companies) for range 48-54 months ago range were extracted into motherduck db")
        print(f"{extract_yfinance_9companies_48_54mo_result}")

        extract_yfinance_9companies_54_60mo_result = extract_yfinance_9companies_54_60mo()
        print("The yfinance data (9 companies) for range 54-60 months ago range were extracted into motherduck db")
        print(f"{extract_yfinance_9companies_54_60mo_result}")

        extract_yfinance_9companies_60_66mo_result = extract_yfinance_9companies_60_66mo()
        print("The yfinance data (9 companies) for range 60-66 months ago range were extracted into motherduck db")
        print(f"{extract_yfinance_9companies_60_66mo_result}")

        extract_yfinance_9companies_66_72mo_result = extract_yfinance_9companies_66_72mo()
        print("The yfinance data (9 companies) for range 66-72 months ago range were extracted into motherduck db")
        print(f"{extract_yfinance_9companies_66_72mo_result}")

        extract_yfinance_9companies_72_78mo_result = extract_yfinance_9companies_72_78mo()
        print("The yfinance data (9 companies) for range 72-78 months ago range were extracted into motherduck db")
        print(f"{extract_yfinance_9companies_72_78mo_result}")

        extract_yfinance_9companies_78_84mo_result = extract_yfinance_9companies_78_84mo()
        print("The yfinance data (9 companies) for range 78-84 months ago range were extracted into motherduck db")
        print(f"{extract_yfinance_9companies_78_84mo_result}")


        transform_load_yfinance_result = transform_load_yfinance(extract_yfinance_9companies_78_84mo_result, "9 companies 78-84 months")
        print("The yfinance data (9 companies) were transformed and stored into motherduck db")
        print(f"{transform_load_yfinance_result}")
        

        # Define batch parameters
        batch_size = 800
        offset = 0
        total_rows = None
        rows_processed = 0  # Initialize rows processed

        # Process each batch until all data is processed
        while total_rows is None or offset < total_rows:
            result = transform_load_yfinance(batch_size=batch_size, offset=offset)
            offset += batch_size  # Update offset for the next batch

            # Set total rows after the first batch to stop loop when all rows are processed
            if total_rows is None and "total_rows" in result:
                total_rows = result["total_rows"]
            
            # Update rows processed
            rows_processed += len(result["rows_processed"]) if "rows_processed" in result else batch_size

            # Send a Slack alert with progress, only if total_rows is known
            if total_rows is not None:
                rows_remaining = max(total_rows - rows_processed, 0)
                progress_message = f"Batch completed: Processed {rows_processed}/{total_rows} rows. {rows_remaining} rows remaining."
                send_slack_alert(progress_message)
                print(progress_message)

        send_slack_alert("******************************** The entire pipeline has successfully completed! ********************************")

    except Exception as e:
        # Send a failure alert in case of any exception
        send_slack_alert(f"ETL Flow encountered an error: {str(e)}")
        send_slack_alert("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! The pipeline ran into an error. !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        raise

# the job
if __name__ == "__main__":
    etl_flow()

