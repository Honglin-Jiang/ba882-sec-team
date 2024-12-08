
# imports
from google.cloud import secretmanager
import requests
import json
import datetime
import uuid
import duckdb
import html
import re
import sec_api
from sec_api import QueryApi, ExtractorApi
import functions_framework

# settings
project_id = 'ba882-team9'
motherduck_secret_id = 'mother_duck'
secapi_secret_id = 'sec-api'
version_id = 'latest'

# db setup
db = 'ba882_team9'
schema = "stage"
db_schema = f"{db}.{schema}"

# Define the companies (ticker symbols)
cloud_companies = ["AMZN", "MSFT", "GOOG", "BABA", "CRM"]
chip_companies = ["INTC", "NVDA", "TSM", "AMD"]
companies = cloud_companies + chip_companies

# Calculate the filing years (last 7 years)
current_year = datetime.datetime.now().year
filing_years = list(range(current_year - 6, current_year + 1))

def get_mdna(ticker, year, queryApi, extractorApi):
    """Fetches the MD&A section from a company's 10-K filing for a given year."""
    try:
        # Get the 10-K filing URL using QueryApi
        filings = queryApi.get_filings(query={
            "query": f"ticker:{ticker} AND formType:\"10-K\"",
            "from": "0",
            "size": "2",
            "sort": [{"filedAt": {"order": "desc"}}],
        })

        # Check if filings list is empty
        if not filings["filings"]:
            print(f"No filings found for {ticker} ({year})")
            return None

        # Proceed if filings are found
        filing_url = filings["filings"][0]["linkToFilingDetails"]

        # Extract the MD&A section (Item 7) using ExtractorApi
        mdna = extractorApi.get_section(filing_url, "7", "text")
        
        # Clean the extracted text
        mdna = html.unescape(mdna)
        mdna = re.sub(r'##TABLE_START|##TABLE_END', '', mdna)
        mdna = re.sub(r'\n\s*\n', '\n', mdna)
        
        return mdna
    except Exception as e:
        print(f"Error fetching MD&A for {ticker} ({year}): {e}")
        return None


@functions_framework.http
def task(request):
    try:
        # Access Secret Manager
        sm = secretmanager.SecretManagerServiceClient()
        
        # Access MotherDuck token
        name = f"projects/{project_id}/secrets/{motherduck_secret_id}/versions/{version_id}"
        response = sm.access_secret_version(request={"name": name})
        md_token = response.payload.data.decode("UTF-8")
        
        # Access sec-api key
        secapi_name = f"projects/{project_id}/secrets/{secapi_secret_id}/versions/{version_id}"
        secapi_response = sm.access_secret_version(request={"name": secapi_name})
        sec_api_key = secapi_response.payload.data.decode("UTF-8")

        # Initialize sec-api services
        queryApi = QueryApi(api_key=sec_api_key)
        extractorApi = ExtractorApi(api_key=sec_api_key)
        
        # Connect to MotherDuck with token
        md = duckdb.connect(f'md:?motherduck_token={md_token}')
        
        # Fetch MD&A data
        mdna = {company: {year: get_mdna(company, year, queryApi, extractorApi) for year in filing_years} for company in companies}

        for company, data in mdna.items():
            for year, mdna_text in data.items():
                if mdna_text:
                    # Generate a unique ID (using UUID here)
                    id = uuid.uuid4()

                    # Check if a record with this ID already exists
                    check_sql = f"""
                        SELECT 1 FROM {db_schema}.K10_new
                        WHERE id = '{id}'  -- Check for duplicate ID
                        LIMIT 1
                    """
                    existing_record = md.sql(check_sql).fetchall()

                    if not existing_record:
                        mdna_text = mdna_text.replace("'", "''")
                        insert_sql = f"""
                            INSERT INTO {db_schema}.K10_new (id, business, date, finan_cond_result_op) 
                            VALUES ('{id}', '{company}', '{year}', '{mdna_text}')
                        """
                        md.sql(insert_sql)
                        print(f"Inserted record for {company} ({year}) with ID {id} into K10_new")
                    else:
                        print(f"Record with ID {id} already exists. Skipping insertion.")

        return {"status": "Success"}, 200

    except Exception as e:
        print(f"Error occurred: {e}")
        return {"error": str(e)}, 500