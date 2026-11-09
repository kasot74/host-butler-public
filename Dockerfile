# host-butler — 本機 Claude API Gateway
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ /app/src/
RUN mkdir -p /app/logs

# 說明用;實際以 network_mode: host 綁 127.0.0.1:${LISTEN_PORT}
EXPOSE 8788

CMD ["python", "src/gateway.py"]
