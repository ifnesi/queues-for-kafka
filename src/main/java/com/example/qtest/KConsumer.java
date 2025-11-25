package com.example.qtest;

import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class KConsumer {

    private static final String TOPIC = "orders-queue";
    private static final AtomicBoolean running = new AtomicBoolean(true);

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "classic-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC));

        System.out.println("✅ Consumer group started. Listening on topic: " + TOPIC);

        final Thread mainThread = Thread.currentThread();

        // Shutdown hook: signal the consumer thread, then wait for it to finish
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("🛑 Shutdown signal received. Waking up consumer...");
            running.set(false);
            consumer.wakeup(); // interrupt poll()

            try {
                // Wait for main thread to finish and execute consumer.close()
                mainThread.join();
            } catch (InterruptedException ignored) {}
        }));

        try {
            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Consumed message: key=%s value=%s partition=%d offset=%d%n",
                            record.key(), record.value(), record.partition(), record.offset());
                }
            }
        } catch (WakeupException e) {
            // Expected on shutdown – only rethrow if we're *not* shutting down
            if (running.get()) {
                throw e;
            }
        } finally {
            System.out.println("🔻 Closing consumer and leaving group...");
            // Close with a timeout to give it a chance to send LeaveGroup & commit
            consumer.close(CloseOptions.timeout(Duration.ofSeconds(10)));
            System.out.println("✅ Consumer closed gracefully.");
        }
    }
}