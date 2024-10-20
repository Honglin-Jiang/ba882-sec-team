# grab the file from the upstream task in the job and parse for loading into raw tables

import functions_framework
from google.cloud import secretmanager
from google.cloud import storage
import duckdb
import json
import pandas as pd
import datetime

# settings
project_id = 'ba882-team9'
secret_id = 'mother_duck'
version_id = 'latest'
bucket_name = 'ba882-team9'


ingest_timestamp = pd.Timestamp.now()


############################################################### helpers

def parse_published(date_str):
    dt_with_tz = datetime.datetime.strptime(date_str, '%a, %d %b %Y %H:%M:%S %z')
    dt_naive = dt_with_tz.replace(tzinfo=None)
    timestamp = pd.Timestamp(dt_naive)
    return timestamp
############################################################### main task


@functions_framework.http
def task(request):
# Parse the request data
    request_json = request.get_json(silent=True)
    print(f"request: {json.dumps(request_json)}")

    # get the jobid and build the gcs base
    job_id = request_json.get('job_id')
    bucket_name = request_json.get('bucket_name')
    gcs_base = f'gs://{bucket_name}/jobs/{job_id}/'

    # instantiate the services
    storage_client = storage.Client()
    sm = secretmanager.SecretManagerServiceClient()  # Instantiate Secret Manager client

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~get the file that triggered this post

    # Access the 'id' from the incoming request
    bucket_name = request_json.get('bucket_name')
    file_path = request_json.get('blob_name')
    print(f"bucket: {bucket_name} and blob name {file_path}")

    # get the file
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_path)
    json_data = blob.download_as_string()
    entries = json.loads(json_data)
    print(f"retrieved {len(entries)} entries")

    # make it a dataframe
    entries_df = pd.DataFrame(entries)
    entries_df['job_id'] = job_id

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ tbl: y_finance

    # Convert the entries to a DataFrame
    yfinance_df = pd.DataFrame(entries)

    # Extract and format the date
    yfinance_df['Date'] = pd.to_datetime(yfinance_df['Date'])

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ DuckDB insertion

    # Build the resource name of the secret version
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret version
    response = sm.access_secret_version(request={"name": name})
    md_token = response.payload.data.decode("UTF-8")

    # Initiate the MotherDuck connection
    md = duckdb.connect(f'md:?motherduck_token={md_token}')

    # Insert data into DuckDB
for index, row in yfinance_df.iterrows():
    insert_sql = f"""
        INSERT INTO your_schema.y_finance (ticker, time, close, volume) 
        VALUES (
            '{row['Ticker']}',  -- Assuming the ticker is in the 'Ticker' column
            '{row['Date'].strftime('%Y-%m-%d')}', 
            {row['Close']}, 
            {row['Volume']}
        )
    """
    md.sql(insert_sql)



    ########################### return
    gcs_links = {
        'yfinance_data': "yfinance data loaded to DuckDB",
    }

    return gcs_links, 200