import functions_framework
from google.cloud import secretmanager
import duckdb
from pinecone import Pinecone, ServerlessSpec

# settings
project_id = 'ba882-team9'
secret_id = 'mother_duck'   #<---------- this is the name of the secret you created
version_id = 'latest'
vector_secret = "pinecone"

# db setup
db = 'ba882_team9'
schema = "stage"
db_schema = f"{db}.{schema}"
vector_index = "mda-content"

@functions_framework.http
def task(request):
    try:
        # instantiate the services 
        sm = secretmanager.SecretManagerServiceClient()

        # Build the resource name of the secret version
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

        # Access the secret version
        response = sm.access_secret_version(request={"name": name})
        md_token = response.payload.data.decode("UTF-8")

        # initiate the MotherDuck connection through an access token through
        md = duckdb.connect(f'md:?motherduck_token={md_token}') 

        ##################################################### create the schema

        # define the DDL statement with an f string
        create_db_sql = f"CREATE DATABASE IF NOT EXISTS {db};"   

        # execute the command to create the database
        md.sql(create_db_sql)

        # confirm it exists
        print(md.sql("SHOW DATABASES").show())

        # create the schema
        md.sql(f"CREATE SCHEMA IF NOT EXISTS {db_schema};") 

        ##################################################### create the core tables in stage

        # y_finance
        raw_tbl_name = f"{db_schema}.y_finance"
        raw_tbl_sql = f"""
        CREATE TABLE IF NOT EXISTS {raw_tbl_name} (
            ticker VARCHAR,
            time TIMESTAMP,
            close FLOAT,
            volume INT
        );
        """
        print(f"{raw_tbl_sql}")
        md.sql(raw_tbl_sql)

        # 10K
        raw_tbl_name = f"{db_schema}.K10_new"
        raw_tbl_sql = f"""
        ALTER TABLE {db_schema}.K10_new ALTER COLUMN id TYPE VARCHAR(36);
        CREATE TABLE IF NOT EXISTS {raw_tbl_name} (
            id INT PRIMARY KEY,  -- Added primary key
            business VARCHAR,
            date VARCHAR,
            finan_cond_result_op VARCHAR,
        );
        """
        
        print(f"{raw_tbl_sql}")
        md.sql(raw_tbl_sql)

        # y_finance_news
        raw_tbl_name = f"{db_schema}.y_finance_news"
        raw_tbl_sql = f"""
        CREATE TABLE IF NOT EXISTS {raw_tbl_name} (
            ticker VARCHAR,
            news_time TIMESTAMP,
            title TEXT,
            url TEXT,
            source VARCHAR
        );
        """
        print(f"{raw_tbl_sql}")
        md.sql(raw_tbl_sql)
        
        ##################################################### vectordb 

        # Build the resource name of the secret version
        vector_name = f"projects/{project_id}/secrets/{vector_secret}/versions/{version_id}"

        # Access the secret version
        response = sm.access_secret_version(request={"name": vector_name})
        pinecone_token = response.payload.data.decode("UTF-8")

        pc = Pinecone(api_key=pinecone_token)

        if not pc.has_index(vector_index):
            pc.create_index(
                name=vector_index,
                dimension=768,
                metric="cosine",
                spec=ServerlessSpec(
                    cloud='aws', # gcp <- not part of free
                    region='us-east-1' # us-central1 <- not part of free
                )
            )

        return {}, 200
    except Exception as e:
            print(f"Error occurred: {e}")
            return {"error": str(e)}, 500

    