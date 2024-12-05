# imports
import streamlit as st
import vertexai
from vertexai.generative_models import GenerativeModel, ChatSession
import duckdb
from google.cloud import secretmanager
import pandas as pd


project_id = 'ba882-team9'
motherduck_secret_id = 'mother_duck'
version_id = "latest"  # 使用最新版本

############################################## streamlit setup


st.image("https://questromworld.bu.edu/ftmba/wp-content/uploads/sites/42/2021/11/Questrom-1-1.png")


# Initialize chat history
if "messages" not in st.session_state:
    st.session_state.messages = []

# App layout
st.title("Text-to-SQL with Google Cloud Vertex AI")
st.subheader("Query your stock price and MD&A data")

# Sidebar filters for demo, not functional
st.sidebar.header("Inputs")
st.sidebar.markdown("One option is to use sidebars for inputs")

user_input = st.text_input("Ask a question about your data:", "Show average stock price for 2021 by company")

############################################## project setup
GCP_PROJECT = 'ba882-team9'
GCP_REGION = "us-central1"

vertexai.init(project=GCP_PROJECT, location=GCP_REGION)


######################################################################## Streamlit App 1 - Simple Conversational Agent


# that chat model
#model = GenerativeModel("gemini-1.5-flash-002")
model = GenerativeModel("gemini-1.5-pro-001")
chat_session = model.start_chat(response_validation=False)

table_schema = """
    Schema: ba882_team9.transformed.y_finance
        - ticker: VARCHAR (Stock ticker symbol)
        - date: DATE (Date of the stock price)
        - close: FLOAT (Closing stock price)
        - volume: INT (Trading volume)

    Schema: ba882_team9.stage.K10
        - business: VARCHAR (Description of the business)
        - date: VARCHAR (Date of the report)
        - finan_cond_result_op: VARCHAR (Summary of financial condition and results of operation)
"""

def generate_sql(prompt, table_schema):
    try:
        # 定義生成模型的 prompt
        full_prompt = f"""
        You are a SQL expert. Based on the following database schema, generate a SQL query:
        
        Schema:
        {table_schema}

        Question:
        {prompt}
        
        Use appropriate date functions for filtering, such as:
        - `YEAR(date)` for filtering by year
        - `YEAR(date)` and `MONTH(date)` for filtering by year and month
        - `date BETWEEN` for filtering by a date range
        - `QUARTER(date)` or `WEEK(date)` if the query mentions quarter or week.

        SQL Query:
        """
        
        # 使用 Vertex AI 生成查詢
        response = model.generate_content(full_prompt)
        sql_query = response.text.strip()
        return sql_query
    except Exception as e:
        st.error(f"Error generating SQL: {str(e)}")
        return None


st.markdown("---")

def correct_date_handling(sql_query):
    # 通用替換規則
    replacements = {
        "CAST(SUBSTRING(date, 1, 4) AS INT)": "YEAR(date)",
        "CAST(SUBSTRING(date, 6, 2) AS INT)": "MONTH(date)",
        "CAST(SUBSTRING(date, 1, 7) AS INT)": "FORMAT(date, 'YYYY-MM')",
        "CAST(QUARTER(date) AS INT)": "QUARTER(date)",
        "CAST(WEEK(date) AS INT)": "WEEK(date)",
    }
    
    # 檢查和替換日期篩選條件
    corrected_query = sql_query
    for old, new in replacements.items():
        corrected_query = corrected_query.replace(old, new)

    # 自動處理 date BETWEEN
    if "BETWEEN" in corrected_query:
        corrected_query = corrected_query.replace(
            "BETWEEN '", ">= DATE('").replace("AND", "AND date <= DATE").replace("'", "')"
        )
    
    return corrected_query


def clean_sql(sql_query):
    # 移除 Markdown 標記和多餘空格
    return sql_query.replace("```sql", "").replace("```", "").strip()


def execute_sql(sql_query):
    try:
        # Access Secret Manager
        sm = secretmanager.SecretManagerServiceClient()
        
        # Access MotherDuck token
        name = f"projects/{project_id}/secrets/{motherduck_secret_id}/versions/{version_id}"
        response = sm.access_secret_version(request={"name": name})
        md_token = response.payload.data.decode("UTF-8")

        # Connect to MotherDuck with token
        md = duckdb.connect(f'md:?motherduck_token={md_token}')
        
        result = md.execute(sql_query).fetchall()

        # Extract column names
        columns = [desc[0] for desc in md.description]
        md.close()
        return result, columns
    except Exception as e:
        st.error(f"Error executing SQL: {str(e)}")
        return None, None


# Main application
if st.button("Generate and Execute SQL"):
    if user_input.strip():
        # Generate SQL query
        sql_query = generate_sql(user_input, table_schema)
        
        if sql_query:
            st.subheader("Generated SQL Query")
            st.code(sql_query, language="sql")
            
            # Execute SQL
            cleaned_sql = clean_sql(sql_query)
            result, columns = execute_sql(cleaned_sql)
            if result:
                st.subheader("Query Results")
                df = pd.DataFrame(result, columns=columns)
                st.write(df)
            else:
                st.info("Query executed successfully but returned no results.")
        else:
            st.error("Failed to generate SQL query.")
    else:
        st.error("Please enter a question.")
