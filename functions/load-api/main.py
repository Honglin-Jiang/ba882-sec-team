# # imports
# import functions_framework
# from google.cloud import secretmanager
# from google.cloud import storage
# import json
# import duckdb
# import pandas as pd

# # settings
# project_id = 'ba882-team9'
# secret_id = 'mother_duck'
# version_id = 'latest'
# bucket_name = 'ba882-team9'

# # db setup
# db = 'ba882_team9'
# schema = "raw"  
# db_schema = f"{db}.{schema}"


# ############################################################### main task

# @functions_framework.http
# def task(request):

#     # Parse the request data
#     request_json = request.get_json(silent=True)
#     print(f"request: {json.dumps(request_json)}")

#     # instantiate the services 
#     sm = secretmanager.SecretManagerServiceClient()
#     storage_client = storage.Client()

#     # Build the resource name of the secret version
#     name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

#     # Access the secret version
#     response = sm.access_secret_version(request={"name": name})
#     md_token = response.payload.data.decode("UTF-8")

#     # initiate the MotherDuck connection through an access token through
#     md = duckdb.connect(f'md:?motherduck_token={md_token}') 

#     # drop if exists and create the raw schema for 
#     #create_schema = f"DROP SCHEMA IF EXISTS {raw_db_schema} CASCADE; CREATE SCHEMA IF NOT EXISTS {raw_db_schema};"
#     create_schema = f"DROP SCHEMA IF EXISTS {db_schema} CASCADE; CREATE SCHEMA IF NOT EXISTS {db_schema};"
#     md.sql(create_schema)

#     print(md.sql("SHOW DATABASES;").show())

#     #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ tbl: K10

# #    # read in from gcs
# #    k10_path = request_json.get('K10')
# #    k10_df = pd.read_parquet(k10_path)

# #    # table logic
# #    raw_tbl_name = f"{raw_db_schema}.K10"
# #    raw_tbl_sql = f"""
# #    DROP TABLE IF EXISTS {raw_tbl_name} ;
# #    CREATE TABLE {raw_tbl_name} AS SELECT * FROM {stage_db_schema}.K10 WHERE FALSE;
# #    """
# #    print(f"{raw_tbl_sql}")
# #    md.sql(raw_tbl_sql)

# #    # ingest into raw schema
# #    ingest_sql = f"INSERT INTO {raw_tbl_name} SELECT * FROM K10_df"
# #    print(f"Import statement: {ingest_sql}")
# #    md.sql(ingest_sql)
# #    del k10_df

# #    # upsert like operation -- will only insert new records, not update
#   #  upsert_sql = f"""
#   #  INSERT INTO {stage_db_schema}.K10 AS stage
#   #  SELECT *
#   #  FROM {raw_tbl_name} AS raw
#  #   ON CONFLICT (business, date)
#  #   DO NOTHING;
#  #   """
#  #   print(upsert_sql)
#  #   md.sql(upsert_sql)
    
    
#     #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ tbl: y_finance

#     # read in from gcs
#     yfinance_path = request_json.get('y_finance')
#     yfinance_df = pd.read_parquet(yfinance_path)

#     # table logic
#     raw_tbl_name = f"{db_schema}.y_finance"
#     raw_tbl_sql = f"""
#     DROP TABLE IF EXISTS {raw_tbl_name} ;
#     CREATE TABLE {raw_tbl_name} AS SELECT * FROM {stage_db_schema}.y_finance WHERE FALSE;
#     """
#     print(f"{raw_tbl_sql}")
#     md.sql(raw_tbl_sql)

#     # ingest into raw schema
#     ingest_sql = f"INSERT INTO {raw_tbl_name} SELECT * FROM y_finance_df"
#     print(f"Import statement: {ingest_sql}")
#     md.sql(ingest_sql)
#     del yfinance_df

#     # upsert like operation -- will only insert new records, not update
#     upsert_sql = f"""
#     INSERT INTO {stage_db_schema}.y_finance AS stage
#     SELECT *
#     FROM {raw_tbl_name} AS raw
#     ON CONFLICT (ticker, time)
#     DO NOTHING;
#     """
#     print(upsert_sql)
#     md.sql(upsert_sql)



#     return {}, 200

# if __name__ == "__main__":
#     app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

# imports
import functions_framework
from google.cloud import secretmanager
from google.cloud import storage
import json
import duckdb
import pandas as pd
import os

# settings
project_id = 'ba882-team9'
secret_id = 'mother_duck'
version_id = 'latest'
bucket_name = 'ba882-team9'

# db setup
db = 'ba882_team9'
schema = "raw"  
db_schema = f"{db}.{schema}"
stage_db_schema = f"{db}.stage"  # Assuming you intend to use the "stage" schema

############################################################### main task

@functions_framework.http
def task(request):

    # Parse the request data
    request_json = request.get_json(silent=True)
    print(f"request: {json.dumps(request_json)}")

    # Instantiate the services 
    sm = secretmanager.SecretManagerServiceClient()
    storage_client = storage.Client()

    # Build the resource name of the secret version
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret version
    response = sm.access_secret_version(request={"name": name})
    md_token = response.payload.data.decode("UTF-8")

    # Initiate the MotherDuck connection through an access token
    md = duckdb.connect(f'md:?motherduck_token={md_token}') 

    # Drop if exists and create the raw schema 
    create_schema = f"DROP SCHEMA IF EXISTS {db_schema} CASCADE; CREATE SCHEMA IF NOT EXISTS {db_schema};"
    md.sql(create_schema)

    print(md.sql("SHOW DATABASES;").show())

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ tbl: y_finance

    # Read in from GCS
    yfinance_path = request_json.get('y_finance')
    yfinance_df = pd.read_parquet(yfinance_path)

    # Table logic
    raw_tbl_name = f"{db_schema}.y_finance"
    raw_tbl_sql = f"""
    DROP TABLE IF EXISTS {raw_tbl_name} ;
    CREATE TABLE {raw_tbl_name} AS SELECT * FROM {stage_db_schema}.y_finance WHERE FALSE;
    """
    print(f"{raw_tbl_sql}")
    md.sql(raw_tbl_sql)

    # Ingest into raw schema
    ingest_sql = f"INSERT INTO {raw_tbl_name} SELECT * FROM y_finance_df"
    print(f"Import statement: {ingest_sql}")
    md.sql(ingest_sql)
    del yfinance_df

    # Upsert-like operation -- will only insert new records, not update
    upsert_sql = f"""
    INSERT INTO {stage_db_schema}.y_finance AS stage
    SELECT *
    FROM {raw_tbl_name} AS raw
    ON CONFLICT (ticker, time)
    DO NOTHING;
    """
    print(upsert_sql)
    md.sql(upsert_sql)

    return {}, 200

# Ensure the app listens on port 8080
if __name__ == "__main__":
    from functions_framework import create_app
    app = create_app('task')
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

