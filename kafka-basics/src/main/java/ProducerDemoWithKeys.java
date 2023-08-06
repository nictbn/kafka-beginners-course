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

public class ProducerDemoWithKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithKeys.class);
    public static final String TOPIC = "demo_java";
    public static final MessageToProducerRecordMapper mapper = new MessageToProducerRecordMapper();

    public static KafkaProducer<String, String> producer = null;

    public static void main(String[] args) {
        log.info("I am a Kafka Producer!");

        // Create Producer Properties
        Properties properties = createProperties();

        // Create the Producer
        producer = new KafkaProducer<>(properties);

        // Send Data
        sendBatches(2,10);

        // Flush and close the producer
        producer.flush();
        producer.close();
    }

    private static Properties createProperties() {
        Properties properties = ProducerPropertiesUtil.getDefaultProperties();
        return properties;
    }

    private static void sendBatches(int numberOfBatches, int batchSize) {
        for (int i = 0; i < numberOfBatches; i++) {
            sendBatch(batchSize);
            sleep(500);
        }
    }

    private static void sendBatch(int batchSize) {
        for (int i = 0; i < batchSize; i++) {
            Message message = createMessage(i);
            sendSingleMessage(message);
        }
    }

    private static void sleep(int milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static Message createMessage(int i) {
        String key = "id_" + i;
        String value = "hello world " + i;
        return new Message(TOPIC, key, value);
    }

    private static void sendSingleMessage(Message message) {
        ProducerRecord<String, String> record = mapper.map(message);
        send(record);
    }

    private static void send(ProducerRecord<String, String> record) {
        producer.send(record, (recordMetadata, e) -> {
            if (e == null) {
                log.info(getLogFormat(record, recordMetadata));
            } else {
                log.error("Error while producing", e);
            }
        });
    }

    private static String getLogFormat(ProducerRecord<String, String> producerRecord, RecordMetadata recordMetadata) {
        return  "Key: " + producerRecord.key() + " | "  + "Partition: " + recordMetadata.partition();
    }
}
