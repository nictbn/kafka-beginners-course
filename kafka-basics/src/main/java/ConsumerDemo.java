import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static util.ConsumerPropertiesUtil.getDefaultProperties;

public class ConsumerDemo {
    public static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class);
    public static final String TOPIC = "demo_java";

    public static void main(String[] args) {
        log.info("I am a Kafka Consumer!");
        Properties properties = getDefaultProperties();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);


        // subscribe to a topic
        consumer.subscribe(List.of(TOPIC));

        // poll for data
        while (true) {
            log.info("Polling");
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                log.info("Key: " + record.key() + ", Value: " + record.value());
                log.info("Partition: " + record.partition() + ", Offser: " + record.offset());
            }
        }
    }


}
