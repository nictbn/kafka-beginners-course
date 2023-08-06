import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static util.ConsumerPropertiesUtil.getDefaultProperties;

public class ConsumerDemoCooperative {
    public static final Logger log = LoggerFactory.getLogger(ConsumerDemoCooperative.class);
    public static final String TOPIC = "demo_java";

    public static void main(String[] args) {
        log.info("I am a Kafka Consumer!");
        Properties properties = getDefaultProperties();
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());
//        properties.setProperty("group.instance.id", "static_id_unique_to_each_consumer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // get a reference to the main thread
        final Thread mainThread = Thread.currentThread();

        // add the shutdown hook
        addShutdownHook(consumer, mainThread);
        // subscribe to a topic

        try {
            consumer.subscribe(List.of(TOPIC));
            // poll for data
            poll(consumer);
        } catch (WakeupException e) {
            log.info("Consumer is starting to shut down");
        } catch (Exception e) {
            log.error("Unexpected exception in the consumer", e);
        } finally {
            consumer.close(); // this will commit the offsets
            log.info("The consumer is now gracefully shut down");
        }
    }

    private static void addShutdownHook(KafkaConsumer<String, String> consumer, Thread mainThread) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Detected a shutdown, let's exist by calling consumer.wakeup()...");
            // the next time we do a poll, a wakeup exception will be thrown
            consumer.wakeup();

            // join the main thread to allow the execution of the code in the main thread
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));
    }

    private static void poll(KafkaConsumer<String, String> consumer) {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                log.info("Key: " + record.key() + ", Value: " + record.value());
                log.info("Partition: " + record.partition() + ", Offser: " + record.offset());
            }
        }
    }
}
