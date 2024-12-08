FROM python:3.9-slim

WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY . .

# Set environment variable for Flask
ENV FLASK_APP=main.py

# Expose the port
EXPOSE 8080

# Command to run the Flask application
CMD ["python", "main.py"]