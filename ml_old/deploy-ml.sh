# set the project
gcloud config set project ba882-team9

echo "======================================================" 
echo "Deploying the ml-sf-predict container" 
echo "======================================================" 
gcloud run deploy ml-sf-predict \ 
    --from-container-image gcr.io/ba882-team9/ml-sf-predict \ 
    --gen2 \
    --runtime=python311 \
    --trigger-http \
    --entry-point=task \
    --region=us-central1 \
    --allow-unauthenticated \
    --memory 2GB \
    --timeout 540s 

echo "======================================================" 
echo "Deploying the ml-sf-train container" 
echo "======================================================" 
gcloud run deploy ml-sf-train \ 
    --from-container-image gcr.io/ba882-team9/ml-sf-train \ 
    --gen2 \
    --runtime=python311 \
    --trigger-http \
    --entry-point=task \
    --region=us-central1 \
    --allow-unauthenticated \
    --memory 2GB \
    --timeout 540s 