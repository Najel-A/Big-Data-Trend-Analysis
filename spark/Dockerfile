FROM bitnami/spark:3.5.1


USER root
RUN apt-get update && apt-get install -y python3-pip


WORKDIR /opt/processing

COPY ../processing /opt/processing


RUN pip3 install --no-cache-dir \
    pyspark \
    kafka-python \
    requests \
    boto3 \
    transformers \
    torch \
    scikit-learn \
    pandas \
    gensim \
    python-dotenv 
# Optional: set default entrypoint (can override in docker-compose)

ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar /opt/bitnami/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar /opt/bitnami/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka_2.12/3.5.1/kafka_2.12-3.5.1.jar /opt/bitnami/spark/jars/


CMD ["spark-submit", "spark_stream.py"]
