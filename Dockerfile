# Use a base Python image
FROM python:3.10

# Set the working directory in the container
WORKDIR /app

# Copy the requirements.txt first, to install dependencies
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# # Copy the service account key file to the container
# COPY ba882-team9-3880f34f3e88.json /app/path-to-credentials.json
# Copy credentials securely
RUN --mount=type=secret,id=gcp-key cp /run/secrets/gcp-key /app/path-to-credentials.json

# Set environment variables for Google Cloud credentials
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/path-to-credentials.json
ENV GOOGLE_CLOUD_PROJECT=ba882-team9

# Set Prefect API URL
ENV PREFECT_API_URL=https://api.prefect.cloud/api/accounts/db2be91e-ac3c-47c7-8afc-9f1f043c2027/workspaces/2d46f809-2085-4220-97da-e282ded15dc7

# Run the script
ENTRYPOINT ["python", "flows/deploy-etl.py"]
