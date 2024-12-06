# Imports
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import duckdb
from google.cloud import secretmanager
import streamlit as st

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
st.title("News Topic Modeling and Sentiment Analysis")
st.subheader("Extract Topics and Sentiment Trends from News Data")

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

def preprocess_text(data):
    """Preprocess text data."""
    return data.fillna("").str.lower()

def perform_topic_modeling(text_data, n_topics=5, n_words=10):
    """Perform topic modeling using LDA."""
    vectorizer = CountVectorizer(stop_words="english", max_df=0.95, min_df=2)
    text_matrix = vectorizer.fit_transform(text_data)
    lda_model = LatentDirichletAllocation(n_components=n_topics, random_state=42)
    lda_model.fit(text_matrix)
    feature_names = vectorizer.get_feature_names_out()
    topics = []
    for topic_idx, topic in enumerate(lda_model.components_):
        topic_words = [feature_names[i] for i in topic.argsort()[:-n_words - 1:-1]]
        topics.append(f"Topic {topic_idx + 1}: {' '.join(topic_words)}")
    return topics

def analyze_sentiment(text):
    """Analyze sentiment using VADER."""
    analyzer = SentimentIntensityAnalyzer()
    scores = analyzer.polarity_scores(text)
    if scores["compound"] > 0.05:
        return "Positive"
    elif scores["compound"] < -0.05:
        return "Negative"
    else:
        return "Neutral"

# Main Workflow
# Step 1: Fetch Data from MotherDuck
news_query = """
    SELECT ticker, news_time, title, url, source
    FROM ba882_team9.stage.y_finance_news
    ORDER BY news_time
"""
news_data = execute_sql(news_query)

# Check if data is available
if not news_data.empty:
    # Step 2: Filter by Ticker
    tickers = news_data["ticker"].unique()
    if "ticker_filter" not in st.session_state:
        st.session_state["ticker_filter"] = "All"

    ticker_filter = st.selectbox(
        "Select Ticker (or 'All' for combined analysis)",
        ["All"] + list(tickers),
        index=(["All"] + list(tickers)).index(st.session_state["ticker_filter"])
    )
    st.session_state["ticker_filter"] = ticker_filter

    if ticker_filter != "All":
        news_data = news_data[news_data["ticker"] == ticker_filter]

    if news_data.empty:
        st.warning(f"No news data available for {ticker_filter}.")
    else:
        # Preprocess text
        news_data["title"] = preprocess_text(news_data["title"])

        # Step 3: Sentiment Analysis
        st.subheader("Sentiment Analysis")
        news_data["sentiment"] = news_data["title"].apply(analyze_sentiment)
        sentiment_counts = news_data["sentiment"].value_counts()
        st.bar_chart(sentiment_counts)

        # Step 4: Topic Modeling
        n_topics = st.number_input("Number of Topics", min_value=2, max_value=10, value=3)
        n_words = st.number_input("Words per Topic", min_value=5, max_value=20, value=10)

        if st.button("Generate Topics"):
            st.subheader("Topic Modeling")
            topics = perform_topic_modeling(news_data["title"], n_topics=n_topics, n_words=n_words)
            for topic in topics:
                st.write(topic)
else:
    st.error("No news data available!")
