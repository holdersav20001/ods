# ODS Glue — Python 3.10 + PySpark 3.3 + S3A (replaces amazon/aws-glue-libs:glue4)
FROM python:3.10-slim-bookworm

# Pinned PostgreSQL JDBC driver — bump in one place to upgrade.
ARG POSTGRES_JDBC_VERSION=42.7.4

RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps \
        curl \
        coreutils \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"
# PySpark 3.3 uses DirectByteBuffer(long,int) — internal constructor present in Java 17
# but removed in Java 21. --add-opens makes it accessible (still exists in 17).
ENV JDK_JAVA_OPTIONS="--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED"

RUN pip install --no-cache-dir \
    pyspark==3.3.2 \
    "confluent-kafka[avro,schemaregistry]==2.3.0" \
    psycopg2-binary==2.9.9 \
    boto3==1.34.0 \
    fastavro==1.9.4 \
    PyYAML==6.0.2 \
    requests==2.31.0

# Download S3A jars and the PostgreSQL JDBC driver into PySpark's jars
# directory so SparkSession picks them up automatically (no --packages needed).
#
# Each download is followed by SHA1 verification using the canonical
# ``.sha1`` checksum file Maven Central publishes alongside every
# artifact (sha256 is not universally hosted; sha1 is). This catches
# in-transit corruption, mirror tampering, and basic supply-chain
# MITM. It does NOT defend against an upstream Maven Central
# compromise — for that we'd need offline-pinned checksums, planned
# as a follow-up once we have a build secrets store.
RUN SPARK_JARS=$(python3 -c "import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), 'jars'))") && \
    cd "${SPARK_JARS}" && \
    for spec in \
        "hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" \
        "aws-java-sdk-bundle-1.12.367.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar" \
        "postgresql-${POSTGRES_JDBC_VERSION}.jar https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
    ; do \
        jar=$(echo "$spec" | cut -d' ' -f1) && \
        url=$(echo "$spec" | cut -d' ' -f2) && \
        curl -fsSL "${url}" -o "${jar}" && \
        expected=$(curl -fsSL "${url}.sha1") && \
        echo "${expected}  ${jar}" | sha1sum -c - ; \
    done

RUN useradd -m -u 1001 -s /bin/bash glue_user

WORKDIR /home/glue_user/workspace/jobs

# Bake job files in — volume mounts at runtime will override for dev
COPY --chown=glue_user:glue_user jobs/ /home/glue_user/workspace/jobs/

USER glue_user

CMD ["echo", "ODS Glue image ready — use DockerOperator to invoke"]
