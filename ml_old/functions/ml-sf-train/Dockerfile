# Use Python 3.10 or later
FROM python:3.10-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install specific version of duckdb that's compatible with MotherDuck
RUN pip install --no-cache-dir duckdb==1.1.1

# Copy the rest of the application
COPY . .

# Set environment variable for Flask
ENV FLASK_APP=main.py

# Expose the port
EXPOSE 8080

# Run the application
CMD exec gunicorn --bind :8080 --workers 1 --threads 8 --timeout 0 main:app