# imports
import requests
from prefect import flow, task
from prefect.futures import wait
from prefect.task_runners import ThreadPoolTaskRunner

# helper function - generic invoker
def invoke_gcf(url:str, payload:dict):
    response = requests.post(url, json=payload)
    response.raise_for_status()
    return response.json()

# setup the schema in the warehouse and vector store (index)
@task(retries=2)
def schema_setup():
    """Setup the stage schema"""
    url = "https://us-central1-ba882-team9.cloudfunctions.net/genai-schema-setup"
    resp = invoke_gcf(url, payload={})
    return resp

# get the posts that haven't been processed
@task(retries=2)
def collect():
    """Collect the posts that haven't been processed"""
    url = "https://us-central1-ba882-team9.cloudfunctions.net/genai-collector"
    resp = invoke_gcf(url, payload={})
    return resp

# process a post passed to it from an upstream task
@task(retries=3)
def ingest(payload):
    """For a given business and date, embed the chunks to support GenAI workflows/"""
    url = "https://us-central1-ba882-team9.cloudfunctions.net/genai-ingestor"
    print(f"Processing business: {payload.get('business_name')}, date: {payload.get('record_date')}")
    resp = invoke_gcf(url, payload=payload)
    return resp


# job that uses the threadpool to process posts in blocks, as they don't have any dependency
# below we define the number of workers at 5
@flow(task_runner=ThreadPoolTaskRunner(max_workers=5), log_prints=True)
def job():

    result = schema_setup()
    print("The schema setup completed")

    collect_result = collect()
    records = collect_result.get("records")  # Get the list of records
    print(f"Records to process: {len(records)}")

    if len(records) > 0:
        print("Starting map operation over the identified records")
        processed_records = []

        for record in records:
            payload = {
                "business_name": record['business'],
                "record_date": record['date']
            }
            processed_records.append(ingest.submit(payload))

        wait(processed_records)

    else:
        print("No records to complete, exiting now.")

# the job
if __name__ == "__main__":
    job()