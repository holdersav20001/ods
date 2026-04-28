FROM confluentinc/cp-kafka-connect:7.6.0

USER root
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.7.4 \
 && confluent-hub install --no-prompt confluentinc/kafka-connect-s3:10.5.7 \
 && confluent-hub install --no-prompt confluentinc/kafka-connect-avro-converter:7.6.0
USER appuser
