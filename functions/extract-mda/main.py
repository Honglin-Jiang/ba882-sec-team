

# imports
from google.cloud import secretmanager
import requests
import json
import datetime
import uuid
import duckdb

# settings
project_id = 'ba882-team9'
secret_id = 'mother_duck'  # <---------- this is the name of the secret you created
version_id = 'latest'

# db setup
db = 'ba882_team9'
schema = "stage"
db_schema = f"{db}.{schema}"

####################################################### helpers
import sec_api
from sec_api import QueryApi, ExtractorApi  # Import both classes
import datetime

# Your API key from sec-api.io
api_key = "791d19143dece943f6431f197570ee4aedaa9f05b68a71e37f3a5ca7c4bfeae0"

# Create instances of the API classes
queryApi = QueryApi(api_key=api_key)
extractorApi = ExtractorApi(api_key=api_key)
# Define the companies (ticker symbols)
cloud_companies = ["AMZN", "MSFT"]
chip_companies = ["INTC"]
#, "GOOG", "BABA", "CRM"
#, "NVDA", "TSM", "AMD"

# Calculate the filing years (last 5 years)
current_year = datetime.datetime.now().year
filing_years = list(range(current_year - 1, current_year + 1))

def get_mdna(ticker, year):
  """Fetches the MD&A section from a company's 10-K filing for a given year."""
  try:
    # Get the 10-K filing URL using QueryApi
    filings = queryApi.get_filings(query={
        "query": f"ticker:{ticker} AND formType:\"10-K\"",
        "from": "0",
        "size": "2",  # Get only the most recent filing for the year
        "sort": [{"filedAt": {"order": "desc"}}],
    })
    filing_url = filings["filings"][0]["linkToFilingDetails"]

    # Extract the MD&A section (Item 7) using ExtractorApi
    mdna = extractorApi.get_section(filing_url, "7", "text")
    return mdna
  except Exception as e:
    print(f"Error fetching MD&A for {ticker} ({year}): {e}")
    return None


import functions_framework

####################################################### core task

@functions_framework.http
def task(request):
    try:
        # Instantiate the services
        sm = secretmanager.SecretManagerServiceClient()

        # Build the resource name of the secret version
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

        # Access the secret version
        response = sm.access_secret_version(request={"name": name})
        md_token = response.payload.data.decode("UTF-8")

        # Initiate the MotherDuck connection through an access token
        md = duckdb.connect(f'md:?motherduck_token={md_token}')

        # job_id
        job_id = datetime.datetime.now().strftime("%Y%m%d%H%M") + "-" + str(uuid.uuid4())

        ####################################### get the feeds
        # Fetch and store the MD&A sections
        cloud_mdna = {}
        for company in cloud_companies:
            cloud_mdna[company] = {}
            for year in filing_years:
                cloud_mdna[company][year] = get_mdna(company, year)

        #chip_mdna = {}
        #for company in chip_companies:
        #    chip_mdna[company] = {}
        #    for year in filing_years:
        #        chip_mdna[company][year] = get_mdna(company, year)

        # Transform and load the data into MotherDuck
# Transform and load the data into MotherDuck
        for company, data in cloud_mdna.items():
            for year, mdna in data.items():
                insert_sql = f"""
                    INSERT INTO {db_schema}.K10 (business, risk_factors, finan_cond_result_op) 
                    VALUES ('{company}', '{year}', '{mdna}') 
                """
                md.sql(insert_sql)

        # Similarly, insert data for chip_mdna

        return {}, 200
    except Exception as e:
        print(f"Error occurred: {e}")
        return {"error": str(e)}, 500