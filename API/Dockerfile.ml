FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.ml.txt /app/
RUN pip install --no-cache-dir -r requirements.ml.txt

RUN mkdir -p /app/models

COPY ml_model2.py /app/

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["python", "ml_model2.py"]