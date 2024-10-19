# imports
from google.cloud import secretmanager
import duckdb
import feedparser
import pandas as pd

# instantiate the service
sm = secretmanager.SecretManagerServiceClient()

# replace below with your own product settings
project_id = 'ba882-team9'
secret_id = 'mother_duck'   #<---------- this is the name of the secret your created above!
version_id = 'latest'

# Build the resource name of the secret version
name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

# Access the secret version
response = sm.access_secret_version(request={"name": name})
md_token = response.payload.data.decode("UTF-8")

# initiate the MotherDuck connection through an access token through
md = duckdb.connect(f'md:?motherduck_token={md_token}') 

# lets look at the high level bits
md.sql("SHOW DATABASES").show()