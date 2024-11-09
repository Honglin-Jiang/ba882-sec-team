#!/bin/bash

# Create/ensure directories exist
mkdir -p ml/functions/ml-sf-predict
mkdir -p ml/functions/ml-sf-train

# Create cloudbuild.yaml in the ml directory
cat > ml/cloudbuild.yaml << 'EOL'
steps:
  # Build and deploy train service
  - name: 'gcr.io/cloud-builders/docker'
    args: ['build', '-t', 'gcr.io/$PROJECT_ID/ml-sf-train', './functions/ml-sf-train']
  
  - name: 'gcr.io/cloud-builders/docker'
    args: ['push', 'gcr.io/$PROJECT_ID/ml-sf-train']
  
  - name: 'gcr.io/cloud-builders/gcloud'
    args:
    - 'run'
    - 'deploy'
    - 'ml-sf-train'
    - '--image'
    - 'gcr.io/$PROJECT_ID/ml-sf-train'
    - '--region'
    - '${_REGION}'
    - '--platform'
    - 'managed'
    - '--set-env-vars'
    - 'MOTHERDUCK_TOKEN=${_MOTHERDUCK_TOKEN},DB_SCHEMA=${_DB_SCHEMA}'

  # Build and deploy predict service
  - name: 'gcr.io/cloud-builders/docker'
    args: ['build', '-t', 'gcr.io/$PROJECT_ID/ml-sf-predict', './functions/ml-sf-predict']
  
  - name: 'gcr.io/cloud-builders/docker'
    args: ['push', 'gcr.io/$PROJECT_ID/ml-sf-predict']
  
  - name: 'gcr.io/cloud-builders/gcloud'
    args:
    - 'run'
    - 'deploy'
    - 'ml-sf-predict'
    - '--image'
    - 'gcr.io/$PROJECT_ID/ml-sf-predict'
    - '--region'
    - '${_REGION}'
    - '--platform'
    - 'managed'
    - '--set-env-vars'
    - 'MOTHERDUCK_TOKEN=${_MOTHERDUCK_TOKEN},DB_SCHEMA=${_DB_SCHEMA}'

images:
  - 'gcr.io/$PROJECT_ID/ml-sf-train'
  - 'gcr.io/$PROJECT_ID/ml-sf-predict'

substitutions:
  _REGION: us-central1
  _DB_SCHEMA: default
EOL

# Create Dockerfile for train service
cat > ml/functions/ml-sf-train/Dockerfile << 'EOL'
FROM python:3.9-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

ENV PORT=8080

CMD ["python", "main.py"]
EOL

# Create Dockerfile for predict service
cat > ml/functions/ml-sf-predict/Dockerfile << 'EOL'
FROM python:3.9-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

ENV PORT=8080

CMD ["python", "main.py"]
EOL

# Create requirements.txt files
cat > ml/functions/ml-sf-train/requirements.txt << 'EOL'
flask==2.0.1
pandas==1.4.0
duckdb==0.9.1
statsmodels==0.13.2
python-dotenv==0.19.2
gunicorn==20.1.0
scikit-learn==1.0.2
numpy>=1.20.0
pickle5==0.0.12
requests==2.31.0
EOL

cat > ml/functions/ml-sf-predict/requirements.txt << 'EOL'
flask==2.0.1
pandas==1.4.0
duckdb==0.9.1
python-dotenv==0.19.2
gunicorn==20.1.0
scikit-learn==1.0.2
numpy>=1.20.0
pickle5==0.0.12
requests==2.31.0
statsmodels==0.13.2
EOL

echo "Project structure created successfully!"