FROM bitnami/spark:3.5.1


USER root
RUN apt-get update && apt-get install -y python3-pip && apt-get install -y curl


WORKDIR /opt/processing

COPY ../processing /opt/processing


RUN pip3 install --no-cache-dir \
    pyspark \
    # kafka-python \
    # requests \
    # boto3 \
    # transformers \
    # torch \
    # scikit-learn \
    pandas \
    gensim \
    python-dotenv 
# Optional: set default entrypoint (can override in docker-compose)

ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar /opt/bitnami/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar /opt/bitnami/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka_2.12/3.5.1/kafka_2.12-3.5.1.jar /opt/bitnami/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar /opt/bitnami/spark/jars/
ADD https://jdbc.postgresql.org/download/postgresql-42.7.3.jar /opt/bitnami/spark/jars/
RUN curl -L -o /opt/bitnami/spark/jars/commons-pool2-2.11.1.jar https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# Change this to the stream we want to use
# Build  a multi stream usage
CMD ["spark-submit", "flajolent_stream.py"]
