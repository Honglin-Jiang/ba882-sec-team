import streamlit as st

st.set_page_config(page_title="BA882 Team9 GenAI Apps", layout="wide")

pg = st.navigation([
    st.Page("assistants.py", title="Simple Chat Assistant", icon=":material/chat:")
    ])
pg.run()