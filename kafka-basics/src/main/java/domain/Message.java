package domain;

public class Message {
    public String topic;
    public String key;
    public String value;

    public Message(String topic, String key, String value) {
        this.topic = topic;
        this.key = key;
        this.value = value;
    }
}
