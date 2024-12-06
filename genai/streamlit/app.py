import streamlit as st

st.set_page_config(page_title="BA882 Team9 GenAI Apps", layout="wide")

pg = st.navigation([
    st.Page("text_to_sql.py", title="AI-Driven Data Query and Visualization Tool", icon=":material/chat:"),
    st.Page("yearly_mda_sentiment_stock_insights.py", title="MD&A Sentiment and Stock Price Trends", icon="📄"),
    st.Page("daily_news_sentiment_stock_analysis.py", title="Daily News Sentiment and Stock Price Trends", icon="📈"),
    st.Page("news_topic_modeling_analysis.py", title="News Topic Modeling Analysis", icon="🗂️")
    ])
pg.run()