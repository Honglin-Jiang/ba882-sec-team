# imports
import streamlit as st
import vertexai
from vertexai.generative_models import GenerativeModel, ChatSession

############################################## streamlit setup


st.image("https://questromworld.bu.edu/ftmba/wp-content/uploads/sites/42/2021/11/Questrom-1-1.png")


# Initialize chat history
if "messages" not in st.session_state:
    st.session_state.messages = []

# App layout
st.title("Streamlit as a GenAI Interface")
st.subheader("Great for Prototypes and POCs")

# Sidebar filters for demo, not functional
st.sidebar.header("Inputs")
st.sidebar.markdown("One option is to use sidebars for inputs")

############################################## project setup
GCP_PROJECT = 'btibert-ba882-fall24'
GCP_REGION = "us-central1"

vertexai.init(project=GCP_PROJECT, location=GCP_REGION)


######################################################################## Streamlit App 1 - Simple Conversational Agent


# that chat model
model = GenerativeModel("gemini-1.5-flash-002")
chat_session = model.start_chat(response_validation=False)

# helper to grab the response
def get_chat_response(chat: ChatSession, prompt: str) -> str:
    text_response = []
    responses = chat.send_message(prompt, stream=True)
    for chunk in responses:
        text_response.append(chunk.text)
    return "".join(text_response)

st.markdown("---")

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])


# React to user input
if prompt := st.chat_input("What is up?"):
    # Display user message in chat message container
    st.chat_message("user").markdown(prompt)
    # Add user message to chat history
    st.session_state.messages.append({"role": "user", "content": prompt})
    # get a response from the chat session with GCP
    response = get_chat_response(chat_session, prompt)
    # playback the response
    with st.chat_message("assistant"):
        st.markdown(response)
    # Add assistant response to chat history
    st.session_state.messages.append({"role": "assistant", "content": response})