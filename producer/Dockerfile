FROM python:3.12-slim

WORKDIR /app

# requirements first so Docker's layer cache skips the pip install
# step on rebuilds where only producer.py changed
COPY producer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# common/ is shared across all four containers -- see the build
# context note in docker-compose.yml for why this path works
COPY common ./common
COPY producer/producer.py .

CMD ["python", "producer.py"]
