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

# Run the script
CMD ["python", "flows/deploy-etl.py"]
