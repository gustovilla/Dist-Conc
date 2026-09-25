# Imagem do processador (consumidor Kafka)
FROM python:3.12-slim

WORKDIR /app

# Dependências primeiro, para aproveitar o cache de camadas do Docker
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY processador.py armazenamento.py consultar.py ./

# Logs aparecem imediatamente em "docker compose logs"
ENV PYTHONUNBUFFERED=1

CMD ["python", "processador.py"]
