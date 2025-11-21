package com.example.qtest;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.clients.consumer.AcknowledgeType;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.Scanner;

/**
 * Minimal example using KafkaShareConsumer (queue/Share group).
 *
 * Start TWO instances of this program (two terminals) to observe queue-style delivery:
 * each message will be given to only one of the consumers.
 */
public class QConsumer {
    private static final String TOPIC = "orders-queue";

    private static void printHeader(String chefName) {
        System.out.println("╔═══════════════════════════════════════════╗");
        System.out.println("║         🔪  StreamBytes Kitchen           ║");
        System.out.println("╠═══════════════════════════════════════════╣");
        System.out.printf("║  Chef: %-34s ║%n", chefName);
        System.out.println("╚═══════════════════════════════════════════╝");
        System.out.println();
    }

    private static void printChef(String chefName) {
        System.out.printf("👨🏻‍🍳 %s listening for new orders...", chefName);
    }

    public static void main(String[] args) {
        String chefName = args.length > 0 ? args[0] : "Unnamed Chef";
        printHeader(chefName);

        Properties props = new Properties();

        // broker address
        props.setProperty("bootstrap.servers", "localhost:9092");
        // group id - same for all consumers that should share messages
        props.setProperty("group.id", "chefs-share-group");
        props.setProperty("share.acknowledgement.mode", "explicit");
        // deserializers
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // (Optional) tune timeouts
        props.setProperty("max.poll.interval.ms", "300000");

        // Create scanner for keyboard input
        Scanner scanner = new Scanner(System.in);

        System.out.println("Creating KafkaShareConsumer...");

        try (KafkaShareConsumer<String, String> consumer = new KafkaShareConsumer<>(props)) {
            consumer.subscribe(Arrays.asList(TOPIC));
            System.out.printf("%n✅ Subscribed to topic '%s' (group='%s')%n",
                TOPIC, props.getProperty("group.id"));
                
            System.out.println();
            printChef(chefName);
            
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                if (records.isEmpty()) {
                    // no records this poll
                    continue;
                }

                for (ConsumerRecord<String, String> r : records) {
                    System.out.printf("%n✨ Received: %s (offset=%d | timestamp=%d | Delivery Count: %d)%n",
                        r.value(), r.offset(), r.timestamp(), r.deliveryCount().get());

                    // Ask user what to do with this message
                    System.out.print("▶️ [A]ccept, [R]eject, R[e]lease... ");
                    System.out.flush();

                    String input = scanner.nextLine().trim().toUpperCase();

                    switch (input) {
                        case "R":
                            System.out.printf("❌ %s rejected!%n", r.value());
                            consumer.acknowledge(r, AcknowledgeType.REJECT);
                            break;
                        case "E":
                            System.out.printf("↩️ %s released back to queue!%n", r.value());
                            consumer.acknowledge(r, AcknowledgeType.RELEASE);
                            //Thread.sleep(1000);
                            break;
                        default:
                            System.out.printf("✅ %s accepted and being prepared!%n", r.value());
                            consumer.acknowledge(r, AcknowledgeType.ACCEPT);
                            //Thread.sleep(1000);
                    }
                    System.out.println();
                    printChef(chefName);
                }

                // Commit the acknowledgements (saves acknowledgement state)
                consumer.commitSync();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        scanner.close();
    }
}