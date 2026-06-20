FROM flink:1.18-scala_2.12-java11

USER root

RUN wget -q -O /opt/flink/lib/flink-sql-connector-kafka.jar \
    "https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar" \
 && wget -q -O /opt/flink/lib/flink-connector-jdbc.jar \
    "https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.18/flink-connector-jdbc-3.1.2-1.18.jar" \
 && wget -q -O /opt/flink/lib/flink-connector-cassandra.jar \
    "https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-cassandra_2.12/3.2.0-1.18/flink-connector-cassandra_2.12-3.2.0-1.18.jar" \
 && wget -q --content-disposition -O /opt/flink/lib/clickhouse-jdbc.jar -L \
    "https://github.com/ClickHouse/clickhouse-java/releases/download/v0.6.5/clickhouse-jdbc-0.6.5-all.jar" \
 && echo "Connectors downloaded OK"

COPY flink-conf.yaml /opt/flink/conf/flink-conf.yaml
COPY entrypoint.sh /entrypoint-xstream.sh
RUN chmod +x /entrypoint-xstream.sh

USER flink

ENTRYPOINT ["/entrypoint-xstream.sh"]
