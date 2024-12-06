# Imports
import streamlit as st
import vertexai
from vertexai.generative_models import GenerativeModel, ChatSession
import duckdb
from google.cloud import secretmanager
import pandas as pd
import plotly.express as px
from io import BytesIO

project_id = 'ba882-team9'
motherduck_secret_id = 'mother_duck'
version_id = "latest"

############################################## Streamlit setup

st.markdown(
    """
    <img src="https://github.com/Honglin-Jiang/ba882-sec-team/blob/main/ba882_team9.jpeg?raw=true" alt="Logo" width="1000">
    """,
    unsafe_allow_html=True
)

# Initialize chat history
if "messages" not in st.session_state:
    st.session_state.messages = []

# App layout
st.title("Text-to-SQL with Google Cloud Vertex AI")
st.subheader("Query your stock price and MD&A data")

# Sidebar filters for demo purposes, not functional
st.sidebar.header("Inputs")
st.sidebar.markdown("One option is to use sidebars for inputs")

user_input = st.text_input("Ask a question about your data:", "Show average stock price for 2021 by company")

############################################## Project setup
GCP_PROJECT = 'ba882-team9'
GCP_REGION = "us-central1"

vertexai.init(project=GCP_PROJECT, location=GCP_REGION)

######################################################################## Streamlit App 1 - Simple Conversational Agent

# Initialize the chat model
# model = GenerativeModel("gemini-1.5-flash-002")
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
        # Define the generation model prompt
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
        
        # Use Vertex AI to generate the query
        response = model.generate_content(full_prompt)
        sql_query = response.text.strip()
        return sql_query
    except Exception as e:
        st.error(f"Error generating SQL: {str(e)}")
        return None


st.markdown("---")

def correct_date_handling(sql_query):
    # General replacement rules
    replacements = {
        "CAST(SUBSTRING(date, 1, 4) AS INT)": "YEAR(date)",
        "CAST(SUBSTRING(date, 6, 2) AS INT)": "MONTH(date)",
        "CAST(SUBSTRING(date, 1, 7) AS INT)": "FORMAT(date, 'YYYY-MM')",
        "CAST(QUARTER(date) AS INT)": "QUARTER(date)",
        "CAST(WEEK(date) AS INT)": "WEEK(date)",
    }
    
    # Check and replace date filters
    corrected_query = sql_query
    for old, new in replacements.items():
        corrected_query = corrected_query.replace(old, new)

    # Automatically handle `date BETWEEN`
    if "BETWEEN" in corrected_query:
        corrected_query = corrected_query.replace(
            "BETWEEN '", ">= DATE('").replace("AND", "AND date <= DATE").replace("'", "')"
        )
    
    return corrected_query


def clean_sql(sql_query):
    # Remove Markdown formatting and extra spaces
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
        # Clear session state for a new query
        st.session_state["query_executed"] = False
        st.session_state.pop("df", None)

        # Generate SQL query
        sql_query = generate_sql(user_input, table_schema)
        
        if sql_query:
            st.subheader("Generated SQL Query")
            st.code(sql_query, language="sql")
            
            # Execute SQL
            cleaned_sql = clean_sql(sql_query)
            result, columns = execute_sql(cleaned_sql)
            if result:
                # Save DataFrame to session state
                st.session_state["df"] = pd.DataFrame(result, columns=columns)
                st.session_state["query_executed"] = True
            else:
                st.info("Query executed successfully but returned no results.")
        else:
            st.error("Failed to generate SQL query.")
    else:
        st.error("Please enter a question.")

# Check if query was executed successfully
if "query_executed" in st.session_state and st.session_state["query_executed"]:
    df = st.session_state["df"]  # Retrieve DataFrame from session state
    
    # Display Query Results
    st.subheader("Query Results")
    st.write(df)

    # Interactive Visualization
    st.subheader("Visualize Query Results")
    visualization_type = st.selectbox("Select a visualization type", ["None", "Bar Chart", "Line Chart", "Scatter Plot"])
    
    if visualization_type != "None":
        x_axis = st.selectbox("Select X-axis", df.columns)
        y_axis = st.selectbox("Select Y-axis", df.columns)
        
        if visualization_type == "Bar Chart":
            fig = px.bar(df, x=x_axis, y=y_axis)
        elif visualization_type == "Line Chart":
            fig = px.line(df, x=x_axis, y=y_axis)
        elif visualization_type == "Scatter Plot":
            fig = px.scatter(df, x=x_axis, y=y_axis)
        
        st.plotly_chart(fig)

    # Export Options
    st.subheader("Export Query Results")
    export_format = st.radio("Select export format", ["CSV", "Excel"])
    
    if export_format == "CSV":
        csv_data = df.to_csv(index=False)
        st.download_button(
            label="Download as CSV",
            data=csv_data,
            file_name="query_results.csv",
            mime="text/csv"
        )
    elif export_format == "Excel":
        # Create Excel file in memory
        from io import BytesIO
        excel_data = BytesIO()
        df.to_excel(excel_data, index=False, engine="openpyxl")
        excel_data.seek(0)
        st.download_button(
            label="Download as Excel",
            data=excel_data,
            file_name="query_results.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
