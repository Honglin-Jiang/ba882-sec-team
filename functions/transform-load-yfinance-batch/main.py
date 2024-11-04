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

@functions_framework.http
def transform_load_yfinance(request):
    """HTTP Cloud Function to transform yfinance data in batches."""
    try:
        # Get batch size and offset from request parameters
        batch_size = int(request.args.get("batch_size", 500))  # Default is 500 rows
        offset = int(request.args.get("offset", 0))

        # Connect to MotherDuck
        md = duckdb.connect(f'md:?motherduck_token={md_token}')
        
        # Step 1: Calculate the total number of rows in the table
        count_sql = f"SELECT COUNT(*) FROM {stage_db_schema}.y_finance"
        total_rows = md.sql(count_sql).fetchone()[0]
        print(f"Total rows in y_finance table: {total_rows}")

        # Step 2: Fetch `batch_size` rows, skipping `offset` rows
        select_sql = f"""
            SELECT * FROM {stage_db_schema}.y_finance 
            LIMIT {batch_size} OFFSET {offset}
        """
        yfinance_df = md.sql(select_sql).df()
        print(f"Fetched {len(yfinance_df)} records from y_finance table with offset {offset}")

        if yfinance_df.empty:
            return {"message": "No more data to process"}, 200

        # Convert the `time` column to `date` and remove duplicates
        yfinance_df = convert_time_to_date(yfinance_df, 'time')
        yfinance_df = yfinance_df.drop_duplicates(subset=['ticker', 'date'])

        # Create target table if it does not already exist
        transformed_tbl_name = f"{transformed_db_schema}.y_finance"
        md.sql(f"CREATE SCHEMA IF NOT EXISTS {transformed_db_schema};")
        md.sql(f"""
            CREATE TABLE IF NOT EXISTS {transformed_tbl_name} (
                ticker VARCHAR,
                date DATE,
                close FLOAT,
                volume INT
            );
        """)

        # Check for existing data before insertion
        for _, row in yfinance_df.iterrows():
            check_sql = f"""
                SELECT COUNT(*) FROM {transformed_tbl_name}
                WHERE ticker = '{row['ticker']}' AND date = '{row['date']}';
            """
            exists = md.sql(check_sql).fetchone()[0] > 0

            if not exists:
                insert_sql = f"""
                    INSERT INTO {transformed_tbl_name} (ticker, date, close, volume)
                    VALUES ('{row['ticker']}', '{row['date']}', {row['close']}, {row['volume']});
                """
                md.sql(insert_sql)
                print(f"Inserted cleaned data for {row['ticker']} on date {row['date']}")
            else:
                print(f"Record already exists for {row['ticker']} on date {row['date']}")

        return {
            "message": f"Processed {batch_size} rows starting from offset {offset}.",
            "next_offset": offset + batch_size,
            "total_rows": total_rows,
        }, 200

    except Exception as e:
        print(f"Error occurred: {e}")
        return {"error": str(e)}, 500
