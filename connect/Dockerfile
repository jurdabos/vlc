# syntax=docker/dockerfile:1

FROM confluentinc/cp-kafka-connect:7.6.1

# Install a specific JDBC Sink version from Confluent Hub and add PostgreSQL JDBC driver
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.2.5 && \
    curl -L -o /usr/share/java/kafka-connect-jdbc/postgresql-42.7.3.jar https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

