
# setup the project
gcloud config set project ba882-team9

echo "======================================================"
echo "build (no cache)"
echo "======================================================"

docker build --no-cache -t gcr.io/ba882-team9/streamlit-rag-app .

echo "======================================================"
echo "push"
echo "======================================================"

docker push gcr.io/ba882-team9/streamlit-rag-app

echo "======================================================"
echo "deploy run"
echo "======================================================"


gcloud run deploy streamlit-rag-app \
    --image gcr.io/ba882-team9/streamlit-rag-app \
    --platform managed \
    --region us-central1 \
    --allow-unauthenticated \
    --service-account ba882-team9@ba882-team9.iam.gserviceaccount.com \
    --memory 1Gi