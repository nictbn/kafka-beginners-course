package util;

import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerPropertiesUtil {
    public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    public static final String LOCALHOST_BOOSTRAP_SERVER = "127.0.0.1:9092";
    public static final String KEY_SERIALIZER = "key.serializer";
    public static final String VALUE_SERIALIZER = "value.serializer";
    public static Properties getDefaultProperties() {
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS, LOCALHOST_BOOSTRAP_SERVER);
        properties.setProperty(KEY_SERIALIZER, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER, StringSerializer.class.getName());
        return properties;
    }
}
