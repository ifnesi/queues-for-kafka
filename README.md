# Queues for Kafka Demo

This is a minimal demo of **Queues for Kafka** (KIP-932), showing how Kafka can be used in a queue-like fashion where messages are delivered to only one consumer within a shared group. The demo simulates a restaurant scenario: waiters (producers) send orders to the kitchen (Kafka topic), and multiple chefs (consumers in the same share group) pick up and process orders one by one.  

The demo is based on the following resources:  
- [KIP-932: Queues for Kafka](https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka)  
- [Morling Dev: KIP-932 Queues for Kafka](https://www.morling.dev/blog/kip-932-queues-for-kafka/)

---

## Dependencies

- [Maven](https://maven.apache.org/install.html)  
- [Java 17](https://www.oracle.com/java/technologies/javase/jdk17-archive-downloads.html)  
- [Docker](https://docs.docker.com/get-docker/)

---

## Setup and Configure Apache Kafka 4.1.0

### Compile the project:
```bash
mvn compile
```

### Pull the Kafka 4.1.0 Docker image:
```bash
docker pull apache/kafka:4.1.0
```

## Running the Demo

### Start Kafka Broker
```bash
docker run --rm -p 9092:9092 apache/kafka:4.1.0
```

### On one terminal, run:
```bash
docker run --rm -p 9092:9092 apache/kafka:4.1.0
```

### Inside the container, execute the following to enable share groups and create the topic:
```bash
opt/kafka/bin/kafka-features.sh --bootstrap-server localhost:9092 upgrade --feature share.version=1
opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic quickstart-events
opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

## Run the Producer

### On another terminal, run:
```bash
mvn -B -q exec:java -Dexec.mainClass=com.example.qtest.QProducer
```
Press ENTER to submit a new order.

## Run the Share Consumers (Chefs)

### Open three separate terminals (one for each chef) and run:
```bash
mvn -B -q exec:java -Dexec.mainClass=com.example.qtest.QTest -Dexec.args="Chef-1"
mvn -B -q exec:java -Dexec.mainClass=com.example.qtest.QTest -Dexec.args="Chef-2"
mvn -B -q exec:java -Dexec.mainClass=com.example.qtest.QTest -Dexec.args="Chef-3"
```

Each chef will receive orders in a queue-style delivery, and you can choose to Accept (A), Release (E), or Reject (R) each order.
