FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

WORKDIR /app

RUN pip install --no-cache-dir \
    fastapi==0.111.0 \
    uvicorn[standard]==0.29.0 \
    boto3==1.34.0 \
    confluent-kafka==2.3.0 \
    psycopg2-binary==2.9.9 \
    fastavro==1.9.4

COPY ods_pipeline /app/ods_pipeline
COPY services /app/services

EXPOSE 8090

CMD ["uvicorn", "services.event_api.main:app", "--host", "0.0.0.0", "--port", "8090"]
