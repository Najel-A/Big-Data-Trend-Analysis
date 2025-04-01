FROM bitnami/spark:latest


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
    gensim

# Optional: set default entrypoint (can override in docker-compose)
CMD ["spark-submit", "spark_stream.py"]
