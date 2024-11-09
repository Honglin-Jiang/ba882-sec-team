# md_utils.py
import os
import duckdb
from google.cloud import secretmanager

# Set up your project and secret info for Google Secret Manager
project_id = 'ba882-team9'
secret_id = 'mother_duck'
version_id = 'latest'

def get_motherduck_token():
    """
    Retrieves the MotherDuck token from Google Secret Manager.
    """
    sm = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = sm.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

def connect_to_motherduck():
    """
    Establishes and returns a connection to the MotherDuck database using the token.
    """
    md_token = get_motherduck_token()
    conn = duckdb.connect(f'md:?motherduck_token={md_token}')
    return conn
