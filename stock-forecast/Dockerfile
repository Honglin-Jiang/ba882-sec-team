FROM python:3.9-slim

WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt gunicorn

# Copy the rest of the application
COPY . .

# Set environment variables
ENV PORT=8080
ENV FLASK_APP=main.py

# Expose the port
EXPOSE 8080

# Use gunicorn to run the Flask app
CMD exec gunicorn --bind :$PORT main:app