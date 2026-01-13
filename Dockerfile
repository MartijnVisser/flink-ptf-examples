FROM apache/flink:2.2-java11

WORKDIR /opt/flink

# Download and install Flink Kafka connector
RUN wget -q -O /tmp/flink-sql-connector-kafka.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/4.0.1-2.0/flink-sql-connector-kafka-4.0.1-2.0.jar && \
    mv /tmp/flink-sql-connector-kafka.jar /opt/flink/lib/

# Download and install Flink SQL Avro Confluent format
RUN wget -q -O /tmp/flink-sql-avro-confluent-registry.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-avro-confluent-registry/2.2.0/flink-sql-avro-confluent-registry-2.2.0.jar && \
    mv /tmp/flink-sql-avro-confluent-registry.jar /opt/flink/lib/

# Note: flink-json (for debezium-json format) is built-in for SQL Client, no download needed

# Copy the PTF JAR file
COPY target/flink-ptf-examples-1.0.2.jar /opt/flink/lib/

# Copy test data for Debezium and DynamoDB examples (accessible at /opt/flink/test-data/)
COPY --chmod=755 test-data/ /opt/flink/test-data/
