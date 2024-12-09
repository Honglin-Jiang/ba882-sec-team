import duckdb

token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6Inl1Y2hlbmx5QGJ1LmVkdSIsInNlc3Npb24iOiJ5dWNoZW5seS5idS5lZHUiLCJwYXQiOiJvQUFnWld6YUxMQUd0RndfdDRKTFMwRlFDbVZFUEtnSzZ5VW14NEluUDhvIiwidXNlcklkIjoiYTI1NGQxMWEtZGNjMy00YTljLThlZGMtZjlmNTc4OWY1ZWY5IiwiaXNzIjoibWRfcGF0IiwiaWF0IjoxNzI4NTg2NDI1fQ.GSJursfAW8KiHiQNWruyCWeEeIUTUUTpCjA0ffnSkkQ"

# Connect to MotherDuck
conn = duckdb.connect(f'md:?token={token}')

# List all schemas and their tables
schemas = ["transformed", "kaggle", "main", "sandbox", "hn", "nyc", "who"]

for schema in schemas:
    try:
        print(f"\nChecking tables in {schema} schema:")
        conn.execute(f"USE {schema}")
        result = conn.execute("SHOW TABLES").fetchall()
        if result:
            for table in result:
                print(f"- {table[0]}")
        else:
            print("No tables found")
    except Exception as e:
        print(f"Error accessing {schema}: {str(e)}")

# Also try to directly query for y_finance table location
print("\nSearching for y_finance table:")
try:
    result = conn.execute("""
        SELECT * 
        FROM information_schema.tables 
        WHERE table_name LIKE '%y_finance%'
    """).fetchall()
    for table in result:
        print(f"Found in: {table[1]}.{table[2]}")
except Exception as e:
    print(f"Error searching for y_finance: {str(e)}")