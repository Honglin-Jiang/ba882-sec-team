# imports
import functions_framework
from google.cloud import secretmanager
import duckdb
import pandas as pd
from pinecone import Pinecone, ServerlessSpec
import json

import vertexai
from vertexai.language_models import TextEmbeddingInput, TextEmbeddingModel

from langchain.text_splitter import RecursiveCharacterTextSplitter


# settings
project_id = 'ba882-team9'
region_id = 'us-central1'
secret_id = 'mother_duck'   #<---------- this is the name of the secret you created
version_id = 'latest'
vector_secret = "pinecone"

# db setup
vector_index = "mda-content"

vertexai.init(project=project_id, location=region_id)

@functions_framework.http
def task(request):

    # Parse the request data
    request_json = request.get_json(silent=True)
    print(f"request: {json.dumps(request_json)}")

    # read in from gcs
    # '409b79b6115c8d051434bfcecf60f69c9f2965e0'
    post_id = request_json.get('post_id')

    # instantiate the services 
    sm = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret version
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret version
    response = sm.access_secret_version(request={"name": name})
    md_token = response.payload.data.decode("UTF-8")

    # initiate the MotherDuck connection through an access token through
    md = duckdb.connect(f'md:?motherduck_token={md_token}') 

    # get the post and change the published timestamp to an int representation for metadata
    post_df = md.sql(f"select * from ba882_team9.stage.K10 where id = '{post_id}' limit 1 ;").df()
      # Assuming your K10 table has 'date' and 'finan_cond_result_op' columns
    post_df['published_timestamp'] = pd.to_datetime(post_df['date']).astype('int64')  # Convert 'date' to timestamp
    post_df['content_text'] = post_df['finan_cond_result_op']  # Use 'finan_cond_result_op' as content

    # connect to pinecone
    vector_name = f"projects/{project_id}/secrets/{vector_secret}/versions/{version_id}"
    response = sm.access_secret_version(request={"name": vector_name})
    pinecone_token = response.payload.data.decode("UTF-8")
    pc = Pinecone(api_key=pinecone_token)
    index = pc.Index(vector_index)
    print(f"index stats: {index.describe_index_stats()}")

    # setup the embedding model
    MODEL_NAME = "text-embedding-005"
    DIMENSIONALITY = 768
    task = "RETRIEVAL_DOCUMENT"
    model = TextEmbeddingModel.from_pretrained(MODEL_NAME)

    # setup the splitter
    chunk_docs = []
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=350,
        chunk_overlap=75,
        length_function=len,
        is_separator_regex=False,
    )

    # iterate over the row
    post_data= post_df.to_dict(orient="records")[0]
    text = post_data['content_text'].replace('\xa0', ' ')
    chunks = text_splitter.create_documents([text])
    id = post_data['id']
    for cid, chunk in enumerate(chunks):
        chunk_text = chunk.page_content
        input = TextEmbeddingInput(chunk_text, task)
        embedding = model.get_embeddings([input])
        chunk_doc = {
            'id': id + '_' + str(cid),
            'values': embedding[0].values,
            'metadata': {
                # 'link': post_data['link'],  # Remove if not applicable
                # 'title': post_data['title'],  # Remove if not applicable
                'published_timestamp': post_data['published_timestamp'],
                'chunk_index': cid,
                'post_id': id,
                'chunk_text': chunk_text
            }
        }
        chunk_docs.append(chunk_doc)
    
    # flatten to dataframe for the ingestion
    chunk_df = pd.DataFrame(chunk_docs)
    print(f"post id {id} has {len(chunk_df)} chunks")

    # upsert to pinecone
    index.upsert_from_dataframe(chunk_df, batch_size=100)

    # add record to warehouse 
    md.sql(f"INSERT INTO ba882-team9.genai.pinecone_posts (id) VALUES ('{id}');")
    print(f"post {id} added to the warehouse or job tracking")

    # finish the job
    return {}, 200