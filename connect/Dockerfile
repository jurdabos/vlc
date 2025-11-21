# syntax=docker/dockerfile:1

# Builder stage: download and prepare connectors
FROM confluentinc/cp-kafka-connect:7.6.1 AS builder

# Install JDBC Sink connector from Confluent Hub
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.2.5

# Download PostgreSQL JDBC driver
RUN curl -L -o /tmp/postgresql-42.7.3.jar https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

# Final stage: minimal runtime image
FROM confluentinc/cp-kafka-connect:7.6.1

# Copy installed connector from builder stage
COPY --from=builder /usr/share/confluent-hub-components/confluentinc-kafka-connect-jdbc /usr/share/confluent-hub-components/confluentinc-kafka-connect-jdbc

# Copy PostgreSQL JDBC driver to connector's lib directory
COPY --from=builder /tmp/postgresql-42.7.3.jar /usr/share/confluent-hub-components/confluentinc-kafka-connect-jdbc/lib/postgresql-42.7.3.jar

# Create non-root user and switch to it
RUN groupadd -r connectuser && useradd -r -g connectuser -u 1000 connectuser && \
    chown -R connectuser:connectuser /usr/share/confluent-hub-components /usr/share/java /etc/kafka-connect /var/log

USER connectuser

