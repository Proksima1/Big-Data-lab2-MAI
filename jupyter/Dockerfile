FROM spark:3.5.2-scala2.12-java17-python3-ubuntu

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    pip3 install --no-cache-dir jupyterlab pyspark==3.5.2 psycopg2-binary clickhouse-connect && \
    curl -fL -o /opt/spark/jars/postgresql-42.7.3.jar https://jdbc.postgresql.org/download/postgresql-42.7.3.jar && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

CMD ["bash", "-lc", "jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token='' --NotebookApp.password=''"]