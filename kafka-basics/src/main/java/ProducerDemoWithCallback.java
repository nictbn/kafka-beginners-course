import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
    public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    public static final String LOCALHOST_BOOSTRAP_SERVER = "127.0.0.1:9092";
    public static final String KEY_SERIALIZER = "key.serializer";
    public static final String VALUE_SERIALIZER = "value.serializer";
    public static final String TOPIC = "demo_java";
    public static final int PRODUCER_BATCH_SIZE = 30;
    public static final int NUMBER_OF_BATCHES = 30;
    public static final String KAFKA_BATCH_SIZE = "batch.size";

    public static KafkaProducer<String, String> producer = null;

    public static void main(String[] args) {
        log.info("I am a Kafka Producer!");

        // Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS, LOCALHOST_BOOSTRAP_SERVER);
        properties.setProperty(KEY_SERIALIZER, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER, StringSerializer.class.getName());

        // KAFKA BATCH SIZE WAS MODIFIED TO DEMONSTRATE THAT MESSAGES ARE SENT USING THE STICKY PARTITIONER IN BATCHES
        // RATHER THAN ROUND ROBIN
        // YOU CAN OBSERVE THIS BEHAVIOUR ONLY IF YOUR TOPIC HAS MULTIPLE PARTITIONS
        properties.setProperty(KAFKA_BATCH_SIZE, "400");

        // Create the Producer
        producer = new KafkaProducer<>(properties);

        // Send Data
        sendBatches(NUMBER_OF_BATCHES, PRODUCER_BATCH_SIZE);

        // Flush and close the producer
        producer.flush();
        producer.close();
    }

    private static void sendBatches(int numberOfBatches, int batchSize) {
        for (int i = 0; i < numberOfBatches; i++) {
            sendBatch(i, batchSize);
        }
    }

    private static void sendBatch(int batchNumber, int batchSize) {
        for (int j = 0; j < batchSize; j++) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC, "hello world " + (batchNumber * batchSize + j));
            sendSingleRecord(producerRecord);
        }
    }

    private static void sendSingleRecord(ProducerRecord<String, String> producerRecord) {
        producer.send(producerRecord, (recordMetadata, e) -> {
            if (e == null) {
                log.info(getLogFormat(recordMetadata));
            } else {
                log.error("Error while producing", e);
            }
        });
    }

    private static String getLogFormat(RecordMetadata recordMetadata) {
        return "Received new metadata\n" +
                "Topic: " + recordMetadata.topic() + "\n" +
                "Partition: " + recordMetadata.partition() + "\n" +
                "Offset: " + recordMetadata.offset() + "\n" +
                "Timestamp: " + recordMetadata.timestamp() + "\n";
    }
}
