import domain.Message;
import mappers.MessageToProducerRecordMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.ProducerPropertiesUtil;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
    public static final String TOPIC = "demo_java";
    public static final int PRODUCER_BATCH_SIZE = 30;
    public static final int NUMBER_OF_BATCHES = 30;
    public static final String KAFKA_BATCH_SIZE = "batch.size";

    public static KafkaProducer<String, String> producer = null;

    public static final MessageToProducerRecordMapper mapper = new MessageToProducerRecordMapper();

    public static void main(String[] args) {
        log.info("I am a Kafka Producer!");

        // Create Producer Properties
        Properties properties = createProperties();

        // Create the Producer
        producer = new KafkaProducer<>(properties);

        // Send Data
        sendBatches(NUMBER_OF_BATCHES, PRODUCER_BATCH_SIZE);

        // Flush and close the producer
        producer.flush();
        producer.close();
    }

    private static Properties createProperties() {
        Properties properties = ProducerPropertiesUtil.getDefaultProperties();

        // KAFKA BATCH SIZE WAS MODIFIED TO DEMONSTRATE THAT MESSAGES ARE SENT USING THE STICKY PARTITIONER IN BATCHES
        // RATHER THAN ROUND ROBIN
        // YOU CAN OBSERVE THIS BEHAVIOUR ONLY IF YOUR TOPIC HAS MULTIPLE PARTITIONS
        properties.setProperty(KAFKA_BATCH_SIZE, "400");
        return properties;
    }

    private static void sendBatches(int numberOfBatches, int batchSize) {
        for (int i = 0; i < numberOfBatches; i++) {
            sendBatch(i, batchSize);
            sleep(500);
        }
    }

    private static void sendBatch(int batchNumber, int batchSize) {
        for (int j = 0; j < batchSize; j++) {
            Message message = createMessage(batchNumber, batchSize, j);
            sendSingleRecord(message);
        }
    }

    private static void sleep(int milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static Message createMessage(int batchNumber, int batchSize, int j) {
        return new Message(TOPIC, null, "hello world " + (batchNumber * batchSize + j));
    }

    private static void sendSingleRecord(Message message) {
        ProducerRecord<String, String> record = mapper.map(message);
        send(record);
    }

    private static void send(ProducerRecord<String, String> record) {
        producer.send(record, (recordMetadata, e) -> {
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
