# Setup the project
gcloud config set project ba882-team9

echo "======================================================"
echo "Deploying the extract-mda function"
echo "======================================================"

gcloud functions deploy extract-mda \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point task \
    --source ./functions/extract-mda \
    --stage-bucket ba882-team9 \
    --service-account ba882-team9@ba882-team9.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 512MB \
    --timeout 60s

echo "======================================================"
echo "Deploying the extract-yfinance function"
echo "======================================================"

gcloud functions deploy extract-yfinance \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point task \
    --source ./functions/extract-yfinance \
    --stage-bucket ba882-team9 \
    --service-account ba882-team9@ba882-team9.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 512MB \
    --timeout 60s

echo "======================================================"
echo "Process Datetime"
echo "======================================================"

gcloud functions deploy parse-api \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point task \
    --source ./functions/parse-api \
    --stage-bucket ba882-team9 \
    --service-account ba882-team9@ba882-team9.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 512MB \
    --timeout 60s