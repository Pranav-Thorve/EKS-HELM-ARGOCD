FROM eclipse-temurin:17-jre

WORKDIR /opt

# Copy Kafka
COPY kafka_2.13-4.1.1.tgz .
RUN tar -xzf kafka_2.13-4.1.1.tgz && \
    mv kafka_2.13-4.1.1 kafka && \
    rm kafka_2.13-4.1.1.tgz

# Copy your custom config
COPY server.properties /opt/kafka/config/server.properties

WORKDIR /opt/kafka

# Kafka port
EXPOSE 9092

# Start Kafka
CMD ["bin/kafka-server-start.sh", "config/server.properties"]
