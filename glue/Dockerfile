FROM amazon/aws-glue-libs:glue4

USER root

RUN pip install --no-cache-dir \
    confluent-kafka==2.3.0 \
    psycopg2-binary==2.9.9 \
    boto3==1.34.0 \
    fastavro==1.9.4 \
    requests==2.31.0

USER glue_user
