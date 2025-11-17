FROM confluentinc/cp-kafka-connect:7.6.1

# Install Confluent JDBC Sink from Confluent Hub
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:latest

# Add PostgreSQL JDBC driver for the JDBC Sink
RUN curl -L -o /usr/share/java/kafka-connect-jdbc/postgresql-42.7.3.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.3.jar
