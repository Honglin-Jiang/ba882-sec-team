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

# @task(retries=2)
# def extract():
#     """Extract the RSS feeds into JSON on GCS"""
#     url = "https://us-central1-btibert-ba882-fall24.cloudfunctions.net/dev-extract-rss"
#     resp = invoke_gcf(url, payload={})
#     return resp

# @task(retries=2)
# def transform(payload):
#     """Process the RSS feed JSON into parquet on GCS"""
#     url = "https://us-central1-btibert-ba882-fall24.cloudfunctions.net/dev-parse-rss"
#     resp = invoke_gcf(url, payload=payload)
#     return resp

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
    
    # extract_result = extract()
    # print("The RSS feeds were extracted onto GCS")
    # print(f"{extract_result}")
    
    # transform_result = transform(extract_result)
    # print("The parsing of the feeds into tables completed")
    # print(f"{transform_result}")

    # result = load(transform_result)
    # print("The data were loaded into the raw schema and changes added to stage")


# the job
if __name__ == "__main__":
    etl_flow()