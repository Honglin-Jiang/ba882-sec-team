# Imports
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from sklearn.linear_model import LinearRegression
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import duckdb
from google.cloud import secretmanager

# Constants
PROJECT_ID = "ba882-team9"
MOTHERDUCK_SECRET_ID = "mother_duck"
VERSION_ID = "latest"

# Streamlit Setup
st.markdown(
    """
    <img src="https://github.com/Honglin-Jiang/ba882-sec-team/blob/main/ba882_team9.jpeg?raw=true" alt="Logo" width="1000">
    """,
    unsafe_allow_html=True
)
st.title("Daily News Sentiment and Stock Analysis")
st.subheader("News Sentiment Analysis and Stock Insights")

# Initialize Sentiment Analyzer
analyzer = SentimentIntensityAnalyzer()

# Helper Functions
def get_motherduck_token():
    """Retrieve MotherDuck token from Google Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{PROJECT_ID}/secrets/{MOTHERDUCK_SECRET_ID}/versions/{VERSION_ID}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

def execute_sql(sql_query):
    """Execute SQL query on MotherDuck using DuckDB."""
    try:
        token = get_motherduck_token()
        md = duckdb.connect(f"md:?motherduck_token={token}")
        result = md.execute(sql_query).fetchall()
        columns = [desc[0] for desc in md.description]
        md.close()
        return pd.DataFrame(result, columns=columns)
    except Exception as e:
        st.error(f"SQL Execution Error: {e}")
        return pd.DataFrame()

def vader_sentiment_analysis(text):
    """Perform sentiment analysis using VADER."""
    if not text or len(text.strip()) == 0:
        return 0.0
    sentiment_scores = analyzer.polarity_scores(text)
    return sentiment_scores.get("compound", 0.0)

def train_regression(X, y):
    """Train a regression model and return coefficients."""
    model = LinearRegression().fit(X, y)
    return model.coef_

# Main Workflow
# Fetch News and Stock Data
news_query = """
    SELECT ticker, news_time, title, source FROM ba882_team9.stage.y_finance_news
    ORDER BY news_time
"""
news_data = execute_sql(news_query)

stock_query = """
    SELECT ticker, date, close, volume FROM ba882_team9.transformed.y_finance
    ORDER BY date
"""
stock_data = execute_sql(stock_query)

if not news_data.empty and not stock_data.empty:
    # Step 1: Select Ticker (Company)
    ticker_options = sorted(news_data["ticker"].unique())
    selected_ticker = st.selectbox(
        "Select a company ticker for analysis",
        ticker_options,
        key="ticker_selector"
    )

    # Filter Data for Selected Ticker
    news_data_filtered = news_data[news_data["ticker"] == selected_ticker]
    stock_data_filtered = stock_data[stock_data["ticker"] == selected_ticker]

    # Sentiment Analysis
    st.subheader(f"Sentiment Analysis for {selected_ticker}")
    news_data_filtered["sentiment"] = news_data_filtered["title"].apply(vader_sentiment_analysis)
    news_data_filtered["news_date"] = pd.to_datetime(news_data_filtered["news_time"]).dt.date
    st.write(news_data_filtered[["news_date", "title", "sentiment"]])

    # Aggregate Sentiment and Stock Data (By Day)
    daily_sentiment = news_data_filtered.groupby("news_date")["sentiment"].mean().reset_index()
    stock_data_filtered["stock_date"] = pd.to_datetime(stock_data_filtered["date"]).dt.date
    merged_data = pd.merge(
        stock_data_filtered,
        daily_sentiment,
        left_on="stock_date",
        right_on="news_date",
        how="inner"
    )

    if not merged_data.empty:
        # Regression Analysis
        st.subheader(f"Impact on Stock Price and Volume for {selected_ticker}")
        
        # Define X (features) and y (targets)
        X = merged_data[["sentiment"]]
        y_price = merged_data["close"]
        y_volume = merged_data["volume"]

        # Perform regression if there is sufficient data
        if len(X) > 0 and len(y_price) > 0:
            try:
                price_coeff = train_regression(X, y_price)
                st.write(f"Impact of Sentiment on Stock Price:  {price_coeff[0]:.2f}")
            except Exception as e:
                st.error(f"Could not compute impact on stock price: {e}")

        if len(X) > 0 and len(y_volume) > 0:
            try:
                volume_coeff = train_regression(X, y_volume)
                st.write(f"Impact of Sentiment on Trading Volume:{volume_coeff[0]:.2f}")
            except Exception as e:
                st.error(f"Could not compute impact on trading volume: {e}")
        
        # Visualizations
        st.subheader(f"Visualizations for {selected_ticker}")

        # Plot Sentiment vs. Stock Price
        if "close" in merged_data and "sentiment" in merged_data:
            fig = go.Figure()
            fig.add_trace(
                go.Scatter(
                    x=merged_data["sentiment"],
                    y=merged_data["close"],
                    mode="markers",
                    name="Sentiment vs. Price",
                )
            )
            fig.update_layout(title="Sentiment vs. Stock Price", xaxis_title="Sentiment", yaxis_title="Stock Price")
            st.plotly_chart(fig)
        
        # Plot Sentiment vs. Trading Volume
        if "volume" in merged_data and "sentiment" in merged_data:
            fig = go.Figure()
            fig.add_trace(
                go.Scatter(
                    x=merged_data["sentiment"],
                    y=merged_data["volume"],
                    mode="markers",
                    name="Sentiment vs. Volume",
                )
            )
            fig.update_layout(title="Sentiment vs. Trading Volume", xaxis_title="Sentiment", yaxis_title="Trading Volume")
            st.plotly_chart(fig)

    else:
        st.info(f"No sufficient data available for {selected_ticker}.")
else:
    st.error("No data available to process!")
