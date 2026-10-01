# Image Airflow + Java + PySpark : les tâches Spark s'exécutent en mode local
# dans le conteneur du scheduler (LocalExecutor).
FROM apache/airflow:2.10.3-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow
ARG AIRFLOW_VERSION=2.10.3
ARG PYTHON_VERSION=3.11
COPY requirements.txt /tmp/requirements.txt
# pyspark pèse ~300 Mo : timeout et retries relevés pour les connexions lentes.
RUN pip install --no-cache-dir --retries 10 --timeout 120 -r /tmp/requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Connecteurs JVM : S3A (versions alignées sur le Hadoop 3.3.4 embarqué par PySpark 3.5)
# et driver JDBC PostgreSQL pour la publication gold.
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262
ARG POSTGRES_JDBC_VERSION=42.7.4
RUN JARS_DIR=$(python -c "import os, pyspark; print(os.path.join(os.path.dirname(pyspark.__file__), 'jars'))") \
    && MAVEN=https://repo1.maven.org/maven2 \
    && curl -fsSL -o "$JARS_DIR/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
        "$MAVEN/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
    && curl -fsSL -o "$JARS_DIR/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" \
        "$MAVEN/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" \
    && curl -fsSL -o "$JARS_DIR/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
        "$MAVEN/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar"

COPY --chown=airflow:root src/ /opt/airflow/src/
COPY --chown=airflow:root dags/ /opt/airflow/dags/
COPY --chown=airflow:root scripts/ /opt/airflow/scripts/
COPY --chown=airflow:root tests/ /opt/airflow/tests/
COPY --chown=airflow:root pytest.ini /opt/airflow/pytest.ini

ENV PYTHONPATH=/opt/airflow/src
