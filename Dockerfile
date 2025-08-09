FROM python:3.11-slim

# Install curl for supercronic download
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install Supercronic (lightweight cron for containers)
# Dockerfile (replace your supercronic RUN with this)
ARG TARGETARCH
ENV SUPERCRONIC_VERSION=v0.2.31

RUN set -eux; \
  case "$TARGETARCH" in \
    amd64) SC=supercronic-linux-amd64 ;; \
    arm64) SC=supercronic-linux-arm64 ;; \
    arm)   SC=supercronic-linux-arm ;; \
    386)   SC=supercronic-linux-386 ;; \
    *) echo "unsupported arch: $TARGETARCH"; exit 1 ;; \
  esac; \
  curl -fsSLo /usr/local/bin/supercronic \
    "https://github.com/aptible/supercronic/releases/download/${SUPERCRONIC_VERSION}/${SC}"; \
  chmod +x /usr/local/bin/supercronic

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# App files
COPY pipeline.py ./pipeline.py
COPY crontab ./crontab

# Default command: just run the pipeline once (good for ad-hoc/manual)
CMD ["python", "pipeline.py"]
