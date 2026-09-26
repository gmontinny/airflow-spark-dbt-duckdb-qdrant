FROM apache/spark:4.0.1

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
      python3 python3-pip netcat-openbsd && \
    pip3 install --no-cache-dir \
      "pandas>=2.0.0" \
      pyarrow \
      "duckdb>=1.0.0" \
      "grpcio>=1.74.0" \
      "grpcio-status>=1.74.0" && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3
ENV SPARK_NO_DAEMONIZE=true

COPY ./conf/spark-defaults.conf "$SPARK_HOME/conf/spark-defaults.conf"

CMD ["/bin/bash", "-c", \
  "$SPARK_HOME/sbin/start-connect-server.sh && tail -f $SPARK_HOME/logs/*"]
