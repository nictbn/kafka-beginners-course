import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {
    public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    public static final String LOCALHOST_BOOSTRAP_SERVER = "127.0.0.1:9092";
    public static final String KEY_SERIALIZER = "key.serializer";
    public static final String VALUE_SERIALIZER = "value.serializer";
    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS, LOCALHOST_BOOSTRAP_SERVER);
        properties.setProperty(KEY_SERIALIZER, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        String topic = "wikimedia.recentchange";
        BackgroundEventHandler eventHandler = new WikimediaChangeHandler(producer, topic);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        EventSource.Builder eventSourceBuilder = new EventSource.Builder(URI.create(url));
        BackgroundEventSource.Builder builder = new BackgroundEventSource.Builder(eventHandler, eventSourceBuilder);
        BackgroundEventSource eventSource = builder.build();

        eventSource.start();
        TimeUnit.MINUTES.sleep(10);
    }
}
