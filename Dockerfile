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

# then app code
COPY . .

# default command (Compose will override, but this is nice locally)
CMD ["python", "src/pipeline.py"]