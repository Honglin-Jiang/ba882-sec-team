# grab the file from the upstream task in the job and parse for loading into raw tables

import functions_framework
from google.cloud import secretmanager
from google.cloud import storage
import duckdb
import json
import pandas as pd
import datetime
from bs4 import BeautifulSoup
import html

# settings
project_id = 'btibert-ba882-fall24'
secret_id = 'mother_duck'   #<---------- this is the name of the secret you created
version_id = 'latest'
bucket_name = 'btibert-ba882-fall24-awsblogs'


ingest_timestamp = pd.Timestamp.now()


############################################################### helpers

def parse_published(date_str):
    dt_with_tz = datetime.datetime.strptime(date_str, '%a, %d %b %Y %H:%M:%S %z')
    dt_naive = dt_with_tz.replace(tzinfo=None)
    timestamp = pd.Timestamp(dt_naive)
    return timestamp


def extract_content_source(content):
    raw_value = content[0].get('value')
    return raw_value


def extract_content_text(content):
    soup = BeautifulSoup(content, 'html.parser')
    cleaned_text = html.unescape(soup.get_text())
    return cleaned_text


def extract_image_data(html_content, post_id, image_info):
    soup = BeautifulSoup(html_content, 'html.parser')
    images = soup.find_all('img')
    
    for i,img in enumerate(images):
        image_data = {
            'post_id': post_id,
            'index': i,
            'src': img.get('src'),
            'width': img.get('width'),
            'height': img.get('height'),

        }
        image_info.append(image_data)


def extract_link_data(html_content, post_id, link_info):
    soup = BeautifulSoup(html_content, 'html.parser')
    links = soup.find_all('a')
    
    for i,link in enumerate(links):
        link_data = {
            'post_id': post_id,
            'index': i,
            'href': link.get('href'),
            'title': link.get_text()

        }
        link_info.append(link_data)


def extract_authors_data(html_content, post_id, authors_info):
    
    # Parse the HTML content
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Find the "About the Authors" section
    authors_section = soup.find_all('h3', string="About the Authors")
    
    if authors_section:
        # Start searching for author entries after the "About the Authors" section
        for i, author in enumerate(authors_section[0].find_next_siblings('p')):
            # Extract author's image
            author_img = author.find('img')
            img_src = author_img.get('src') if author_img else None
            
            # Extract author's name
            author_name = author.find('strong').get_text(strip=True) if author.find('strong') else None
            
            # Extract author's biography (the remaining text after the strong tag)
            bio_text = author.get_text(separator=" ", strip=True)
            if author_name:
                # Remove the name from the bio to get only the bio text
                bio_text = bio_text.replace(author_name, '', 1).strip()
            
            # Append the author data to the list
            author_data = {
                'post_id': post_id,
                'index': i,
                'name': author_name,
                'image': img_src,
                'bio': bio_text
            }
            authors_info.append(author_data)


############################################################### main task


@functions_framework.http
def task(request):

    # Parse the request data
    request_json = request.get_json(silent=True)
    print(f"request: {json.dumps(request_json)}")

    # get the jobid and build the gcs base
    job_id = request_json.get('job_id')
    bucket_name = request_json.get('bucket_name')
    gcs_base = f'gs://{bucket_name}/jobs/{job_id}/'

    # instantiate the services 
    storage_client = storage.Client()

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~get the file that triggered this post

    # Access the 'id' from the incoming request
    bucket_name = request_json.get('bucket_name')
    file_path = request_json.get('blob_name')
    print(f"bucket: {bucket_name} and blob name {file_path}")

    # get the file
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_path)
    json_data = blob.download_as_string()
    entries = json.loads(json_data)
    print(f"retrieved {len(entries)} entries")

    # make it a dataframe
    entries_df = pd.DataFrame(entries)
    entries_df['job_id'] = job_id

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ tbl: tags

    # iterrows is not ideal, but with a job getting about 160 entries in a dataframe, this isn't bad
    tag_rows = []
    for _, row in entries_df.iterrows():
        for entry in row['tags']:
            new_row = entry.copy()  # Copy the dictionary
            new_row['post_id'] = row['id']  # Retain the original id
            new_row['job_id'] = row['job_id']  # Retain the original jobid
            tag_rows.append(new_row)
    tags_df = pd.DataFrame(tag_rows) 
    tags_df = tags_df[['term', 'post_id', 'job_id']]  # a couple keys always appear blank
    tags_df['ingest_timestamp'] = ingest_timestamp
    print(f"tags were flatted to shape: {tags_df.shape}") 

    # write to gcs
    tags_fpath = gcs_base + "tags.parquet"
    tags_df.to_parquet(tags_fpath, engine='pyarrow')


    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ tbl: posts

    # copy the object
    posts_df = entries_df.copy()

    # some light transforms on datetimes for post timestamp and ingestion
    posts_df['published'] = posts_df['published'].apply(parse_published)
    posts_df['ingest_timestamp'] = ingest_timestamp

    # the content isolated
    posts_df['content_source'] = posts_df['content'].apply(extract_content_source)
    posts_df['content_text'] = posts_df['content_source'].apply(extract_content_text)

    # keep the columns we want, some data loss but we are ok with that
    keep_cols = [
        'id', 
        'link', 
        'title',  
        'summary', 
        'content_source',
        'content_text', 
        'job_id',
        'published', 
        'ingest_timestamp', 
    ]
    posts_df = posts_df[keep_cols]

    # write to gcs
    posts_fpath = gcs_base + "posts.parquet"
    posts_df.to_parquet(posts_fpath, engine='pyarrow')


    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ tbl: images
    # NOTE: may not need a mapping table, just store the data repeated

    # apply the function to each row in the DataFrame
    image_info = []
    for index, row in posts_df.iterrows():
        extract_image_data(row['content_source'], row['id'], image_info)
    
    # flatten into a dataframe
    images_df = pd.DataFrame(image_info)

    # light cleanup - a handful may not have dimensions
    images_df = images_df.dropna(subset="src")
    images_df['width'] = pd.to_numeric(images_df['width'], errors='coerce')
    images_df['height'] = pd.to_numeric(images_df['height'], errors='coerce')
    images_df['ingest_timestamp'] = ingest_timestamp
    images_df['job_id'] = job_id

    # write to gcs
    images_fpath = gcs_base + "images.parquet"
    images_df.to_parquet(images_fpath, engine='pyarrow')


    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ tbl: embedded links

    # apply the function to each row in the DataFrame
    link_info = []
    for index, row in posts_df.iterrows():
        extract_link_data(row['content_source'], row['id'], link_info)
    
    links_df = pd.DataFrame(link_info)
    links_df['ingest_timestamp'] = ingest_timestamp
    links_df['job_id'] = job_id

    # write to gcs
    links_fpath = gcs_base + "links.parquet"
    links_df.to_parquet(links_fpath, engine='pyarrow')

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ tbl: authors
    
    authors_info = []
    for index, row in posts_df.iterrows():
        extract_authors_data(row['content_source'], row['id'], authors_info)
    
    authors_df = pd.DataFrame(authors_info)

    # it looks like the parser may add blank entries, but it's safe to delete them
    # postid = 43675ea64f5f36ab7475b039aa322936010b0947. <--- all were captured, but parsed an extra with no info
    authors_df = authors_df.dropna(subset=['name', 'image'])
    authors_df['ingest_timestamp'] = ingest_timestamp
    authors_df['job_id'] = job_id
    
    # write to gcs
    authors_fpath = gcs_base + "authors.parquet"
    authors_df.to_parquet(authors_fpath, engine='pyarrow')


    ########################### return
    gcs_links = {
        'posts': posts_fpath,
        'tags': tags_fpath,
        'authors': authors_fpath,
        'images': images_fpath,
        'links': links_fpath,
    }

    return gcs_links, 200