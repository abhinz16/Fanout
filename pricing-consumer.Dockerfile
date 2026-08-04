FROM python:3.12-slim

WORKDIR /app

COPY consumers/pricing/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common ./common
COPY consumers/pricing/pricing_consumer.py .

CMD ["python", "pricing_consumer.py"]
