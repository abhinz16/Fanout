FROM python:3.12-slim

WORKDIR /app

COPY consumers/risk/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common ./common
COPY consumers/risk/risk_consumer.py .

CMD ["python", "risk_consumer.py"]
