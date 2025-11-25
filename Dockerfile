FROM flink:2.1.0-scala_2.12-java11

# Set working directory
WORKDIR /opt/flink

# Download and install Flink SQL Kafka Connector 4.0.1-2.0
# Note: For SQL usage, we need flink-sql-connector-kafka, not flink-connector-kafka
RUN wget -q -O /tmp/flink-sql-connector-kafka.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/4.0.1-2.0/flink-sql-connector-kafka-4.0.1-2.0.jar && \
    mv /tmp/flink-sql-connector-kafka.jar /opt/flink/lib/

# Download and install Flink SQL Avro Confluent Registry
# Note: For SQL usage, we need flink-sql-avro-confluent-registry, not flink-avro-confluent-registry
RUN wget -q -O /tmp/flink-sql-avro-confluent-registry.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-avro-confluent-registry/2.1.0/flink-sql-avro-confluent-registry-2.1.0.jar && \
    mv /tmp/flink-sql-avro-confluent-registry.jar /opt/flink/lib/

# Copy the PTF JAR file
COPY target/flink-ptf-examples-1.0.1.jar /opt/flink/lib/
