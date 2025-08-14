FROM python:3.11-slim

# Install necessary system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application files
COPY pipeline.py ./pipeline.py

# Default command: run the ETL pipeline
CMD ["python", "pipeline.py"]