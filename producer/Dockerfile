FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY log_setup.py .
COPY metrics.py .
COPY finnhub_producer.py .
COPY scylla_consumer.py .

ENV CONFIG_PATH=/app/config.yaml

CMD ["python", "-u", "finnhub_producer.py"]
