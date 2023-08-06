package mappers;

import domain.Message;
import org.apache.kafka.clients.producer.ProducerRecord;

public class MessageToProducerRecordMapper {
    public ProducerRecord<String, String> map(Message message) {
        return new ProducerRecord<>(message.topic, message.key, message.value);
    }
}
