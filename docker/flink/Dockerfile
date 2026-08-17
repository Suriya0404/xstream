FROM flink:1.18-scala_2.12-java11

USER root

# PyFlink runtime so the JobManager can run generated `.py` jobs via `flink run -py`.
# apache-flink MUST match the 1.18 base image.
RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    python3 python3-pip python3-dev python-is-python3 \
 && rm -rf /var/lib/apt/lists/* \
 && pip3 install --no-cache-dir apache-flink==1.18.1

RUN wget -q -O /opt/flink/lib/flink-sql-connector-kafka.jar \
    "https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar" \
 && wget -q -O /opt/flink/lib/flink-connector-jdbc.jar \
    "https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.18/flink-connector-jdbc-3.1.2-1.18.jar" \
 && wget -q -O /opt/flink/lib/flink-connector-cassandra.jar \
    "https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-cassandra_2.12/3.2.0-1.18/flink-connector-cassandra_2.12-3.2.0-1.18.jar" \
 && wget -q --content-disposition -O /opt/flink/lib/clickhouse-jdbc.jar -L \
    "https://github.com/ClickHouse/clickhouse-java/releases/download/v0.6.5/clickhouse-jdbc-0.6.5-all.jar" \
 && wget -q -O /opt/flink/lib/flink-sql-connector-mongodb.jar \
    "https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-mongodb/1.2.0-1.18/flink-sql-connector-mongodb-1.2.0-1.18.jar" \
 && echo "Connectors downloaded OK"

COPY flink-conf.yaml /opt/flink/conf/flink-conf.yaml
COPY entrypoint.sh /entrypoint-xstream.sh
RUN chmod +x /entrypoint-xstream.sh

# Checkpoint dir must be writable by the flink user. Creating + chowning it here
# makes a freshly-created named volume inherit flink ownership (Docker seeds a new
# volume from the image path's contents/permissions).
RUN mkdir -p /opt/flink/checkpoints && chown -R flink:flink /opt/flink/checkpoints

USER flink

ENTRYPOINT ["/entrypoint-xstream.sh"]
