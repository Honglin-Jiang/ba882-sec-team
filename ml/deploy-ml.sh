# Set the Google Cloud project
gcloud config set project btibert-ba882-fall24

echo "======================================================"
echo "Deploying the stock price training function"
echo "======================================================"

# Deploy the training function
gcloud functions deploy stock-price-train \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point task \
    --source ./functions/stock_price_train \
    --stage-bucket ba882-team9 \
    --service-account ba882-team9@ba882-team9.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 2GB  \
    --timeout 300s 

echo "======================================================"
echo "Deploying the stock price inference function"
echo "======================================================"

# Deploy the inference function
gcloud functions deploy stock-price-serve \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point task \
    --source ./functions/stock_price_serve \
    --stage-bucket ba882-team9 \
    --service-account ba882-team9@ba882-team9.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 1GB  \
    --timeout 60s
