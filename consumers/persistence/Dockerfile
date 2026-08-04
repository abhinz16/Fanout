FROM python:3.12-slim

WORKDIR /app

COPY consumers/persistence/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common ./common
COPY consumers/persistence/persistence_consumer.py .

CMD ["python", "persistence_consumer.py"]
