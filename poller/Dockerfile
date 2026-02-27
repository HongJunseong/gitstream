FROM python:3.11-slim

WORKDIR /app

# System deps required by confluent-kafka (librdkafka)
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc libssl-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy poller package (github_poller.py + utils/)
COPY poller/ .

# Emit logs unbuffered so they appear immediately in `docker logs`
ENV PYTHONUNBUFFERED=1

CMD ["python", "-u", "github_poller.py"]
