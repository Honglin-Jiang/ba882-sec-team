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

gcloud functions deploy extract-yfinance-100companies-3mo \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point task \
    --source ./functions/extract-yfinance-100companies-3mo \
    --stage-bucket ba882-team9 \
    --service-account ba882-team9@ba882-team9.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 1024MB \
    --timeout 540s


gcloud functions deploy extract-yfinance-100companies-3-6mo \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point task \
    --source ./functions/extract-yfinance-100companies-3-6mo \
    --stage-bucket ba882-team9 \
    --service-account ba882-team9@ba882-team9.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 1024MB \
    --timeout 540s


gcloud functions deploy extract-yfinance-100companies-6-9mo \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point task \
    --source ./functions/extract-yfinance-100companies-6-9mo \
    --stage-bucket ba882-team9 \
    --service-account ba882-team9@ba882-team9.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 1024MB \
    --timeout 540s

gcloud functions deploy extract-yfinance-100companies-9-12mo \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point task \
    --source ./functions/extract-yfinance-100companies-9-12mo \
    --stage-bucket ba882-team9 \
    --service-account ba882-team9@ba882-team9.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 1024MB \
    --timeout 540s

gcloud functions deploy extract-yfinance-100companies-12-15mo \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point task \
    --source ./functions/extract-yfinance-100companies-12-15mo \
    --stage-bucket ba882-team9 \
    --service-account ba882-team9@ba882-team9.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 1024MB \
    --timeout 540s

gcloud functions deploy extract-yfinance-100companies-15-18mo \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point task \
    --source ./functions/extract-yfinance-100companies-15-18mo \
    --stage-bucket ba882-team9 \
    --service-account ba882-team9@ba882-team9.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 1024MB \
    --timeout 540s

gcloud functions deploy extract-yfinance-100companies-18-21mo \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point task \
    --source ./functions/extract-yfinance-100companies-18-21mo \
    --stage-bucket ba882-team9 \
    --service-account ba882-team9@ba882-team9.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 1024MB \
    --timeout 540s

gcloud functions deploy extract-yfinance-100companies-21-24mo \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point task \
    --source ./functions/extract-yfinance-100companies-21-24mo \
    --stage-bucket ba882-team9 \
    --service-account ba882-team9@ba882-team9.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 1024MB \
    --timeout 540s