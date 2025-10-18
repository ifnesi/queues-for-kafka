package com.example.qtest;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Simple Kafka producer that sends one message at a time.
 * Each message is of the form "Order: <ORDER_ID>".
 * User presses ENTER to send a new message, Ctrl-C to quit.
 */
public class QProducer {

    private static final String TOPIC = "quickstart-events";

    private static void printHeader() {
        System.out.println("╔═══════════════════════════════════════════╗");
        System.out.println("║         🍽️  StreamBytes Restaurant         ║");
        System.out.println("╠═══════════════════════════════════════════╣");
        System.out.println("║  Waiters send orders to the kitchen via   ║");
        System.out.println("║  Kafka Queues (KIP-932 / Share Groups).   ║");
        System.out.println("╚═══════════════════════════════════════════╝");
        System.out.println();
    }

    public static void main(String[] args) {
        printHeader();

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        AtomicBoolean running = new AtomicBoolean(true);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props);
            Scanner scanner = new Scanner(System.in)) {
            // Handle Ctrl-C gracefully
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("\nShutdown signal received...");
                running.set(false);
                try {
                    producer.flush();
                } catch (Exception ignored) {}
                producer.close();
                System.out.println("Producer closed.");
            }));

            Random random = new Random();

            System.out.println();
            System.out.println();
            while (running.get()) {
                System.out.printf("🛎️ Press [ENTER] to send a new order ([Ctrl+C] to quit)...");
                if (!scanner.hasNextLine()) {
                    break; // Input closed or Ctrl-D
                }
                scanner.nextLine();

                if (!running.get()) break; // Exit if shutdown signal triggered

                // Generate 4 random digits (0000–9999)
                String orderId = String.format("%04d", random.nextInt(10000));
                String value = "Order #" + orderId;

                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, value);
                try {
                    RecordMetadata metadata = producer.send(record).get();
                    System.out.printf("Sent: %s (partition=%d | offset=%d)%n%n",
                            value, metadata.partition(), metadata.offset());
                } catch (Exception e) {
                    System.err.println("Failed to send message: " + e.getMessage());
                    System.out.println();
                }
            }

            System.out.println("Exiting main loop...");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}