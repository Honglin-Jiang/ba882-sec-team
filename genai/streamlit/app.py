import streamlit as st

st.set_page_config(page_title="BA882 Team9 GenAI Apps", layout="wide")

pg = st.navigation([
    st.Page("text_to_sql.py", title="AI-Driven Data Query and Visualization Tool", icon=":material/chat:"),
    st.Page("sentiment_driven_stock_insights.py", title="MD&A Sentiment and Stock Price Trends", icon="📄")
    ])
pg.run()