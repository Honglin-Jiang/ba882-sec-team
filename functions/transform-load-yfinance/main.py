import functions_framework
from google.cloud import secretmanager
import duckdb
import pandas as pd
import datetime

# settings
project_id = 'ba882-team9'
secret_id = 'mother_duck'
version_id = 'latest'
db = 'ba882_team9'
stage_schema = 'stage'
transformed_schema = 'transformed'
stage_db_schema = f'{db}.{stage_schema}'
transformed_db_schema = f'{db}.{transformed_schema}'

ingest_timestamp = pd.Timestamp.now()

############################################################### helpers

#def convert_time_to_year(df, time_column):
#    """Convert timestamp to year."""
#    df[time_column] = pd.to_datetime(df[time_column])
#    df['year'] = df[time_column].dt.year
#    return df

def convert_time_to_date(df, time_column):
    """Convert timestamp to date (year, month, day)."""
    df[time_column] = pd.to_datetime(df[time_column])
    df['date'] = df[time_column].dt.date
    return df

############################################################### main task
'''
@functions_framework.http
def task(request):
    """HTTP Cloud Function to process yfinance data and store yearly aggregated data into transformed schema."""
    try:
        # Instantiate the Secret Manager service
        sm = secretmanager.SecretManagerServiceClient()

        # Build the resource name of the secret version
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

        # Access the secret version
        response = sm.access_secret_version(request={"name": name})
        md_token = response.payload.data.decode("UTF-8")

        # Connect to MotherDuck
        md = duckdb.connect(f'md:?motherduck_token={md_token}')
        
        # Step 1: Retrieve data from MotherDuck
        select_sql = f"SELECT * FROM {stage_db_schema}.y_finance"
        yfinance_df = md.sql(select_sql).df()  # Read as a pandas DataFrame
        print(f"Fetched {len(yfinance_df)} records from y_finance table")

        if yfinance_df.empty:
            return {"error": "No data found in y_finance table"}, 404

        # Step 2: Convert time column to year
        yfinance_df = convert_time_to_year(yfinance_df, 'time')
        
        # Step 3: Group by ticker and year, calculate average close and volume
        yearly_avg = yfinance_df.groupby(['ticker', 'year']).agg({
            'close': 'mean',
            'volume': 'mean'
        }).reset_index()

        # Rename columns for clarity
        yearly_avg.rename(columns={'close': 'avg_close', 'volume': 'avg_volume'}, inplace=True)

        # Step 4: Create the transformed table if it doesn't exist
        transformed_tbl_name = f"{transformed_db_schema}.y_finance"
        md.sql(f"""
            CREATE SCHEMA IF NOT EXISTS {transformed_db_schema};
        """)
        md.sql(f"""
            CREATE TABLE IF NOT EXISTS {transformed_tbl_name} (
                ticker VARCHAR,
                year INT,
                avg_close FLOAT,
                avg_volume FLOAT
            );
        """)

        # Step 5: Insert aggregated data into transformed table
        for _, row in yearly_avg.iterrows():
            insert_sql = f"""
                INSERT INTO {transformed_tbl_name} (ticker, year, avg_close, avg_volume)
                VALUES ('{row['ticker']}', {row['year']}, {row['avg_close']}, {row['avg_volume']});
            """
            md.sql(insert_sql)
            print(f"Inserted aggregated data for {row['ticker']} in year {row['year']}")

        return {"message": "YFinance data successfully transformed and inserted into transformed schema"}, 200

    except Exception as e:
        print(f"Error occurred: {e}")
        return {"error": str(e)}, 500
'''
'''
@functions_framework.http
def task(request):
    """HTTP Cloud Function to process yfinance data and store cleaned data into transformed schema."""
    try:
        # Instantiate the Secret Manager service
        sm = secretmanager.SecretManagerServiceClient()

        # Build the resource name of the secret version
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

        # Access the secret version
        response = sm.access_secret_version(request={"name": name})
        md_token = response.payload.data.decode("UTF-8")

        # Connect to MotherDuck
        md = duckdb.connect(f'md:?motherduck_token={md_token}')
        
        # Step 1: Retrieve data from MotherDuck
        select_sql = f"SELECT * FROM {stage_db_schema}.y_finance"
        yfinance_df = md.sql(select_sql).df()  # Read as a pandas DataFrame
        print(f"Fetched {len(yfinance_df)} records from y_finance table")

        if yfinance_df.empty:
            return {"error": "No data found in y_finance table"}, 404

        # Step 2: Convert time column to date
        yfinance_df = convert_time_to_date(yfinance_df, 'time')

        # Step 3: Remove duplicate rows based on 'ticker' and 'date'
        yfinance_df = yfinance_df.drop_duplicates(subset=['ticker', 'date'])

        # Step 4: Create the transformed table if it doesn't exist
        transformed_tbl_name = f"{transformed_db_schema}.y_finance"
        md.sql(f"""
            CREATE SCHEMA IF NOT EXISTS {transformed_db_schema};
        """)
        md.sql(f"""
            CREATE TABLE IF NOT EXISTS {transformed_tbl_name} (
                ticker VARCHAR,
                date DATE,
                close FLOAT,
                volume INT
            );
        """)

        # Step 5: Insert cleaned data into the transformed table if not already present
        for _, row in yfinance_df.iterrows():
            # Check if a record with the same ticker and date already exists
            check_sql = f"""
                SELECT COUNT(*) FROM {transformed_tbl_name}
                WHERE ticker = '{row['ticker']}' AND date = '{row['date']}';
            """
            exists = md.sql(check_sql).fetchone()[0] > 0

            if not exists:  # Insert only if the record doesn't exist
                insert_sql = f"""
                    INSERT INTO {transformed_tbl_name} (ticker, date, close, volume)
                    VALUES ('{row['ticker']}', '{row['date']}', {row['close']}, {row['volume']});
                """
                md.sql(insert_sql)
                print(f"Inserted cleaned data for {row['ticker']} on date {row['date']}")
            else:
                print(f"Record already exists for {row['ticker']} on date {row['date']}")

        return {"message": "YFinance data successfully cleaned and inserted into transformed schema"}, 200

    except Exception as e:
        print(f"Error occurred: {e}")
        return {"error": str(e)}, 500
'''
'''
@functions_framework.http
def task(request):
    """HTTP Cloud Function to process yfinance data and store cleaned data into transformed schema."""
    try:
        # Instantiate the Secret Manager service
        sm = secretmanager.SecretManagerServiceClient()

        # Build the resource name of the secret version
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

        # Access the secret version
        response = sm.access_secret_version(request={"name": name})

        # Debug print to inspect the raw payload data
        print("Secret payload data:", response.payload.data)  # Add this line to see what data is being fetched

        # Decode the secret token
        md_token = response.payload.data.decode("UTF-8")

        # Connect to MotherDuck
        md = duckdb.connect(f'md:?motherduck_token={md_token}')
        
        # Step 1: Retrieve data from MotherDuck
        select_sql = f"SELECT * FROM {stage_db_schema}.y_finance"
        yfinance_df = md.sql(select_sql).df()  # Read as a pandas DataFrame
        print(f"Fetched {len(yfinance_df)} records from y_finance table")

        if yfinance_df.empty:
            return {"error": "No data found in y_finance table"}, 404

        # Step 2: Convert time column to date
        yfinance_df = convert_time_to_date(yfinance_df, 'time')

        # Step 3: Remove duplicate rows based on 'ticker' and 'date'
        yfinance_df = yfinance_df.drop_duplicates(subset=['ticker', 'date'])

        # Step 4: Create the transformed table if it doesn't exist
        transformed_tbl_name = f"{transformed_db_schema}.y_finance"
        md.sql(f"""
            CREATE SCHEMA IF NOT EXISTS {transformed_db_schema};
        """)
        md.sql(f"""
            CREATE TABLE IF NOT EXISTS {transformed_tbl_name} (
                ticker VARCHAR,
                date DATE,
                close FLOAT,
                volume INT
            );
        """)

        # Step 5: Insert cleaned data into the transformed table if not already present
        for _, row in yfinance_df.iterrows():
            # Check if a record with the same ticker and date already exists
            check_sql = f"""
                SELECT COUNT(*) FROM {transformed_tbl_name}
                WHERE ticker = '{row['ticker']}' AND date = '{row['date']}';
            """
            exists = md.sql(check_sql).fetchone()[0] > 0

            if not exists:  # Insert only if the record doesn't exist
                insert_sql = f"""
                    INSERT INTO {transformed_tbl_name} (ticker, date, close, volume)
                    VALUES ('{row['ticker']}', '{row['date']}', {row['close']}, {row['volume']});
                """
                md.sql(insert_sql)
                print(f"Inserted cleaned data for {row['ticker']} on date {row['date']}")
            else:
                print(f"Record already exists for {row['ticker']} on date {row['date']}")

        return {"message": "YFinance data successfully cleaned and inserted into transformed schema"}, 200

    except Exception as e:
        print(f"Error occurred: {e}")
        return {"error": str(e)}, 500
'''


# Define date range (7 days)
seven_days_ago = (datetime.datetime.now() - datetime.timedelta(days=7)).strftime('%Y-%m-%d')


@functions_framework.http
def task(request):
    """HTTP Cloud Function to process yfinance data and store cleaned data into transformed schema"""
    try:
        # Create Secret Manager client
        sm = secretmanager.SecretManagerServiceClient()

        # Build resource name for the secret version
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

        # Access the secret version
        response = sm.access_secret_version(request={"name": name})
        md_token = response.payload.data.decode("UTF-8")

        # Connect to MotherDuck
        md = duckdb.connect(f'md:?motherduck_token={md_token}')

        # Step 1: Select only data from the last 7 days
        select_sql = f"SELECT * FROM {stage_db_schema}.y_finance WHERE time >= '{seven_days_ago}'"
        yfinance_df = md.sql(select_sql).df()  # Read as a pandas DataFrame
        print(f"Fetched {len(yfinance_df)} records from y_finance table within the last 7 days")

        if yfinance_df.empty:
            return {"error": "No data found in y_finance table"}, 404

        # Step 2: Convert the time column to date
        yfinance_df = convert_time_to_date(yfinance_df, 'time')

        # Step 3: Remove duplicate rows based on 'ticker' and 'date'
        yfinance_df = yfinance_df.drop_duplicates(subset=['ticker', 'date'])

        # Step 4: Create the transformed table if it doesn't exist
        transformed_tbl_name = f"{transformed_db_schema}.y_finance"
        md.sql(f"""
            CREATE SCHEMA IF NOT EXISTS {transformed_db_schema};
        """)
        md.sql(f"""
            CREATE TABLE IF NOT EXISTS {transformed_tbl_name} (
                ticker VARCHAR,
                date DATE,
                close FLOAT,
                volume INT
            );
        """)

        # Step 5: Insert cleaned data into the transformed table, if not already present
        for _, row in yfinance_df.iterrows():
            # Check if a record with the same ticker and date already exists in the transformed table
            check_sql = f"""
                SELECT COUNT(*) FROM {transformed_tbl_name}
                WHERE ticker = '{row['ticker']}' AND date = '{row['date']}';
            """
            exists = md.sql(check_sql).fetchone()[0] > 0

            if not exists:  # Insert only if the record doesn't exist
                insert_sql = f"""
                    INSERT INTO {transformed_tbl_name} (ticker, date, close, volume)
                    VALUES ('{row['ticker']}', '{row['date']}', {row['close']}, {row['volume']});
                """
                md.sql(insert_sql)
                print(f"Inserted cleaned data for {row['ticker']} on date {row['date']}")
            else:
                print(f"Record already exists for {row['ticker']} on date {row['date']}")

        return {"message": "YFinance data successfully cleaned and inserted into transformed schema"}, 200

    except Exception as e:
        print(f"Error occurred: {e}")
        return {"error": str(e)}, 500