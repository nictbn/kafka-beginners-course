package util;

import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class ConsumerPropertiesUtil {

    public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    public static final String LOCALHOST_BOOTSTRAP_SERVER = "localhost:9092";
    public static final String KEY_DESERIALIZER = "key.deserializer";
    public static final String VALUE_DESERIALIZER = "value.deserializer";
    public static final String GROUP_ID = "group.id";
    public static final String DEFAULT_GROUP_ID = "my-java-application";
    public static final String AUTO_OFFSET_RESET = "auto.offset.reset";
    public static final String AUTO_OFFSET_RESET_STRATEGY = "earliest";

    public static Properties getDefaultProperties() {
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS, LOCALHOST_BOOTSTRAP_SERVER);
        properties.setProperty(KEY_DESERIALIZER, StringDeserializer.class.getName());
        properties.setProperty(VALUE_DESERIALIZER, StringDeserializer.class.getName());
        properties.setProperty(GROUP_ID, DEFAULT_GROUP_ID);
        properties.setProperty(AUTO_OFFSET_RESET, AUTO_OFFSET_RESET_STRATEGY);
        return properties;
    }
}
