from prefect import flow

if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/Honglin-Jiang/ba882-sec-team.git",
        entrypoint="genai/pipeline/flows/ingest-posts.py:job",
    ).deploy(
        name="genai-post-ingestion",
        work_pool_name="honglin-pool1",
        job_variables={"env": {"BROCK": "loves-to-code"},
                       "pip_packages": ["pandas", "requests"]},
        cron="15 1 * * *",
        tags=["prod"],
        description="The pipeline to grab unprocessed posts and process to store in the vector database",
        version="1.0.0",
    )