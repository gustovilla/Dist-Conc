# Imagem do sensor (produtor Kafka)
FROM python:3.12-slim

WORKDIR /app

# Dependências primeiro, para aproveitar o cache de camadas do Docker
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY sensor.py .

# Logs aparecem imediatamente em "docker compose logs"
ENV PYTHONUNBUFFERED=1

CMD ["python", "sensor.py"]
