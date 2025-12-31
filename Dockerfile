FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY . .

RUN python -m pip install --upgrade pip && \
    python -m pip install --no-cache-dir .

CMD ["arp-jarvis-run-coordinator", "--host", "0.0.0.0", "--port", "8081"]
