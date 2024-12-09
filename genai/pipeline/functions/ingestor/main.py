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
secret_id = 'mother_duck'  # <---------- this is the name of the secret you created
version_id = 'latest'
vector_secret = "pinecone"

# db setup
vector_index = "post-content"

vertexai.init(project=project_id, location=region_id)


@functions_framework.http
def task(request):

    # Parse the request data
    request_json = request.get_json(silent=True)
    print(f"request: {json.dumps(request_json)}")

    # Assuming 'business_name' and 'record_date' are passed in the request
    business_name = request_json.get('business_name')
    record_date = request_json.get('record_date')

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
    post_df = md.sql(f"""
        SELECT * 
        FROM ba882_team9.stage.K10 
        WHERE business = '{business_name}' AND date = '{record_date}' 
        LIMIT 1
    """).df()
    if post_df.empty:
        return {"error": "No matching record found in K10"}, 404

    post_df['timestamp'] = pd.to_datetime(post_df['date']).astype('int64')

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
    post_data = post_df.to_dict(orient="records")[0]
    text = post_data['finan_cond_result_op'].replace('\xa0', ' ')
    chunks = text_splitter.create_documents([text])

    for cid, chunk in enumerate(chunks):
        chunk_text = chunk.page_content
        input = TextEmbeddingInput(chunk_text, task)
        embedding = model.get_embeddings([input])
        chunk_doc = {
            'id': business_name + '_' + record_date + '_' + str(cid),  # Use business_name and record_date in the ID
            'values': embedding[0].values,
            'metadata': {
                'published_timestamp': post_data['timestamp'],
                'chunk_index': cid,
                'chunk_text': chunk_text
            }
        }
        chunk_docs.append(chunk_doc)

    # flatten to dataframe for the ingestion
    chunk_df = pd.DataFrame(chunk_docs)
    print(f"post id {business_name}_{record_date} has {len(chunk_df)} chunks")

    # upsert to pinecone
    index.upsert_from_dataframe(chunk_df, batch_size=100)

    # add record to warehouse with business_name and record_date
    md.sql(f"""
        INSERT INTO ba882_team9.genai.pinecone_posts (business, date) 
        VALUES ('{business_name}', '{record_date}');
    """)
    print(f"Record added to pinecone_posts with business {business_name} and date {record_date}")

    # finish the job
    return {}, 200