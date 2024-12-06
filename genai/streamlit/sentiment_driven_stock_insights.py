# Imports
import streamlit as st
import vertexai
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# Google Cloud and MotherDuck configurations
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
st.title("Sentiment-Driven Stock Insights")
st.subheader("Analyze MD&A sentiment and its impact on stock price trends")

############################################## Initialize Vertex AI and sentiment analyzer
GCP_PROJECT = 'ba882-team9'
GCP_REGION = "us-central1"
vertexai.init(project=GCP_PROJECT, location=GCP_REGION)
analyzer = SentimentIntensityAnalyzer()

############################################## Initialize session state
if "selected_company" not in st.session_state:
    st.session_state.selected_company = None

############################################## Helper functions
# Execute SQL query on DuckDB with MotherDuck
def execute_sql(sql_query):
    try:
        # Access Secret Manager
        from google.cloud import secretmanager
        sm = secretmanager.SecretManagerServiceClient()
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

# Sentiment analysis function
def analyze_sentiment(text, debug=False):
    if not text or len(text.strip()) == 0:
        return 0.0
    if debug:
        st.text(f"Analyzing: {text[:100]}")
    sentiment_scores = analyzer.polarity_scores(text)
    return sentiment_scores.get("compound", 0.0)

############################################## Main functionality
# Step 1: Fetch Available Companies
companies_query = "SELECT DISTINCT business FROM ba882_team9.stage.K10 ORDER BY business"
companies, _ = execute_sql(companies_query)
if companies:
    company_options = sorted([row[0] for row in companies])
else:
    st.error("No companies found in the database.")
    company_options = []

# Step 2: Select Company
selected_company = st.selectbox(
    "Select a company for analysis",
    company_options,
    index=company_options.index(st.session_state.selected_company)
    if st.session_state.selected_company in company_options
    else 0,
    key="company_selector",
)
if selected_company != st.session_state.selected_company:
    st.session_state.selected_company = selected_company

# Step 3: Fetch Sentiment Data Across All Years
mda_query = f"""
    SELECT CAST(date AS INTEGER) AS year, finan_cond_result_op
    FROM ba882_team9.stage.K10
    WHERE business = '{selected_company}'
    ORDER BY year
"""
mda_result, mda_columns = execute_sql(mda_query)


if mda_result:
    mda_df = pd.DataFrame(mda_result, columns=mda_columns)

    # Apply sentiment analysis to the financial condition report
    mda_df["sentiment"] = mda_df["finan_cond_result_op"].apply(analyze_sentiment)

    # Group by year to get average sentiment score
    mda_df = mda_df.groupby("year").agg({"sentiment": "mean"}).reset_index()


    # Step 4: Fetch Stock Price and Volume Data
    stock_query = f"""
        SELECT CAST(EXTRACT(YEAR FROM date) AS INTEGER) AS year, AVG(close) AS avg_price, SUM(volume) AS total_volume
        FROM ba882_team9.transformed.y_finance
        WHERE ticker = '{selected_company}'
        GROUP BY year
        ORDER BY year
    """
    stock_result, stock_columns = execute_sql(stock_query)

    if stock_result:
        stock_df = pd.DataFrame(stock_result, columns=stock_columns)

        # Step 5: Merge Sentiment and Stock Data
        merged_df = pd.merge(mda_df, stock_df, on="year", how="inner")

        # Step 6: Visualize Sentiment, Stock Price, and Volume
        st.subheader(f"Sentiment, Stock Price, and Volume Trends for {selected_company}")

        # Chart 1: Sentiment Score (Bar) + Stock Price (Line)
        fig1 = go.Figure()

        # Sentiment Score (Bar)
        fig1.add_trace(
            go.Bar(
                x=merged_df["year"],
                y=merged_df["sentiment"],
                name="Sentiment Score",
                marker=dict(color="rgba(0, 123, 255, 0.7)"),
            )
        )

        # Average Stock Price (Line)
        fig1.add_trace(
            go.Scatter(
                x=merged_df["year"],
                y=merged_df["avg_price"],
                mode="lines+markers",
                name="Average Stock Price",
                line=dict(color="green"),
                yaxis="y2",
            )
        )

        # Layout for Chart 1
        fig1.update_layout(
            title=f"Sentiment Score and Stock Price Trends for {selected_company}",
            xaxis=dict(title="Year"),
            yaxis=dict(
                title="Sentiment Score",
                side="left",
                titlefont=dict(color="blue"),
                tickfont=dict(color="blue"),
                showgrid=True,  # Enable gridlines for the left axis
            ),
            yaxis2=dict(
                title="Stock Price",
                side="right",
                overlaying="y",
                titlefont=dict(color="green"),
                tickfont=dict(color="green"),
                showgrid=False,  # Disable gridlines for the right axis
            ),
            legend=dict(orientation="h", y=-0.3),
        )


        # Display Chart 1
        st.plotly_chart(fig1)

        # Chart 2: Sentiment Score (Bar) + Total Trading Volume (Line)
        fig2 = go.Figure()

        # Sentiment Score (Bar)
        fig2.add_trace(
            go.Bar(
                x=merged_df["year"],
                y=merged_df["sentiment"],
                name="Sentiment Score",
                marker=dict(color="rgba(0, 123, 255, 0.7)"),
            )
        )

        # Total Trading Volume (Line)
        fig2.add_trace(
            go.Scatter(
                x=merged_df["year"],
                y=merged_df["total_volume"],
                mode="lines+markers",
                name="Total Trading Volume",
                line=dict(color="orange"),
                yaxis="y2",
            )
        )

        # Layout for Chart 2
        fig2.update_layout(
            title=f"Sentiment Score and Trading Volume Trends for {selected_company}",
            xaxis=dict(title="Year"),
            yaxis=dict(
                title="Sentiment Score",
                side="left",
                titlefont=dict(color="blue"),
                tickfont=dict(color="blue"),
                showgrid=True,
            ),
            yaxis2=dict(
                title="Trading Volume",
                side="right",
                overlaying="y",
                titlefont=dict(color="orange"),
                tickfont=dict(color="orange"),
                showgrid=False,
            ),
            legend=dict(orientation="h", y=-0.3),
        )
            
        # Display Chart 2
        st.plotly_chart(fig2)

    else:
        st.info("No stock price data available for the selected company.")
else:
    st.info("No MD&A data found for the selected company.")