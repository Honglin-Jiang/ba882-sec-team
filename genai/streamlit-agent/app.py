# imports
import streamlit as st 
import json
import networkx as nx 
import matplotlib.pyplot as plt

from google.cloud import secretmanager
import vertexai
from vertexai.generative_models import GenerativeModel, Part
from vertexai.language_models import TextEmbeddingInput, TextEmbeddingModel
from vertexai.generative_models import GenerationConfig

GCP_PROJECT = 'ba882-team9'
project_id = 'ba882-team9'
GCP_REGION = "us-central1"

from pinecone import Pinecone, ServerlessSpec
import duckdb

version_id = 'latest'
vector_secret = "pinecone"
vector_index = 'post-content'
EMBEDDING_MODEL = "text-embedding-005"

# secret manager pinecone
sm = secretmanager.SecretManagerServiceClient()
vector_name = f"projects/{project_id}/secrets/{vector_secret}/versions/{version_id}"
response = sm.access_secret_version(request={"name": vector_name})
pinecone_token = response.payload.data.decode("UTF-8")
pc = Pinecone(api_key=pinecone_token)
index = pc.Index(vector_index)
# print(f"index stats: {index.describe_index_stats()}")

# secret manager pinecone
vector_name = f"projects/{project_id}/secrets/mother_duck/versions/{version_id}"
response = sm.access_secret_version(request={"name": vector_name})
md_token = response.payload.data.decode("UTF-8")
md = duckdb.connect(f'md:?motherduck_token={md_token}')

# vertex ai
vertexai.init(project=project_id, location=GCP_REGION)
llm = GenerativeModel("gemini-1.5-pro-001", )
embedder = TextEmbeddingModel.from_pretrained(EMBEDDING_MODEL)
TASK = "RETRIEVAL_QUERY"


############################################## streamlit setup


st.image("https://questromworld.bu.edu/ftmba/wp-content/uploads/sites/42/2021/11/Questrom-1-1.png")
st.title("Project RAG App")


################### sidebar <---- I know, we can do other layouts, easy for POCs

st.sidebar.title("Search the blogs")

search_query = st.sidebar.text_area("What are you looking to learn?")

top_k = st.sidebar.slider("Top K", 1, 15, step=1)

search_button = st.sidebar.button("Run the RAG pipeline")


################### main

# Main action: Handle search
if search_button:
    if search_query.strip():
        # Get embedding
        embedding_input = TextEmbeddingInput(text=search_query, task_type=TASK)
        embedding_result = embedder.get_embeddings([embedding_input])
        embedding = embedding_result[0].values  # Assuming one input, get the embedding vector

        # search pincone
        results = index.query(
            vector=embedding,
            top_k=top_k,
            include_metadata=True
        )

        # answer the question
        chunks = [r.metadata['chunk_text'] for r in results.matches]
        print(results)
        context = "\n".join(chunks)

        prompt_template = f"""
            You are an AI assistant trained to provide detailed and accurate answers based on the provided context.
            Use the context below to respond to the query.
            If the context does not contain sufficient information to answer, state that explicitly and avoid making up an answer.

            ### Context:
            {context}

            ### Query:
            {search_query}

            ### Your Response:
            """

        response = llm.generate_content(
            prompt_template,
            generation_config=GenerationConfig(temperature=0)
        )

        # Display the results
        st.write(response.text)

        # # return the full document from just the first entry - 
        # top_article = results.matches[0]['metadata']['link']
        # post_id = results.matches[0]['metadata']['post_id']
        # st.write(top_article)

        # # the full blog post
        # st.markdown("---")
        # SQL = f"""select    title, 
        #                     content_source, 
        #                     content_text,
        #                     LISTAGG(t.term, ', ') AS tags
        #         from        awsblogs.stage.posts  p
        #         left join   awsblogs.stage.tags t on p.id = t.post_id
        #         where id =  '{post_id}'
        #         group by    1,2,3
        # """
        # print(SQL)
        # page = md.sql(SQL).df()
        # st.markdown("## The 'Top Ranked Article' from Pinecone")
        # st.info("This is the parent article for the top chunk")
        # st.markdown(f"### {page.title.to_list()[0]}")
        # st.pills('Tags', options=page.tags.to_list()[0].split(','))
        # st.markdown(page.content_source.to_list()[0], unsafe_allow_html=True)

        # # the knowledge graph from the article
        # st.markdown("---")
        # kg_template = f"""
        # Extract a knowledge graph from the given blog post article.  A knowledge graph consists of triplets, or two entities connected by a directed relationship.  It is possible for an entity to be identified multiple times in the article.

        # Return the data as a JSON document with a list of tuples in the form of (entity1, relationship, entity2).  The JSON document should have a single top-level key called "knowledge_graph", and the value associated with this key should be a list of JSON objects.  Each JSON object represents a triplet and should have the following keys: "entity1", "relationship", and "entity2".  The values associated with these keys should be strings representing the entities and the relationship type.

        # For example, if the blog post article contains the sentence "Albert Einstein was a physicist who developed the theory of relativity.", a possible triplet could be ("Albert Einstein", "developed", "theory of relativity").  Another possible triplet could be ("Albert Einstein", "was a", "physicist").

        # Please ensure that the extracted entities and relationships are relevant to the main topic of the blog post article.  If an entity is mentioned multiple times in the article, you can include it in multiple triplets.

        # # JSON SCHEMA
        # - a key called knowledge graph, the value is a list of triples

        # For example, the entry below only has one triple, but you can return many
        # ('Albert Einstein', 'DEVELOPED', 'Theory of Relativity')

        # # REQUIREMENTS:
        # - only extract 25 triples

        # # Blog Post Article:
        # {page.content_text.to_list()[0]} 
        # """

        # kg_resp = llm.generate_content(
        #     kg_template,
        #     generation_config=GenerationConfig(temperature=0,
        #                                        response_mime_type="application/json"),
        # )

        # resp_json = json.loads(kg_resp.text)
        # print(resp_json)

        # kg = resp_json.get("knowledge_graph", [])
        # print(len(kg))

        # G = nx.DiGraph()
        # for triple in kg:
        #     G.add_edge(triple['entity1'], triple['entity2'], label=triple['relationship'])
        
        # pos = nx.spring_layout(G)  # Position nodes for visualization
        # fig = plt.figure(figsize=(8, 6))
        # nx.draw(G, pos, with_labels=True, node_size=3000, node_color="lightblue", font_size=10, font_weight="bold", arrowsize=15)

        # # Add edge labels for relationships
        # edge_labels = {(u, v): d["label"] for u, v, d in G.edges(data=True)}
        # nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels, font_color="red", font_size=9)

        # plt.title("Knowedge Graph")
        # st.pyplot(fig)


    else:
        st.warning("Please enter a search query!")