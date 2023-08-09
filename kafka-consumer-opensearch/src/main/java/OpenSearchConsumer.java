import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {

    public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    public static final String LOCALHOST_BOOTSTRAP_SERVER = "localhost:9092";
    public static final String KEY_DESERIALIZER = "key.deserializer";
    public static final String VALUE_DESERIALIZER = "value.deserializer";
    public static final String GROUP_ID = "group.id";
    public static final String DEFAULT_GROUP_ID = "consumer-opensearch-demo";
    public static final String AUTO_OFFSET_RESET = "auto.offset.reset";
    public static final String AUTO_OFFSET_RESET_STRATEGY = "latest";
    public static final String INDEX_NAME = "wikimedia";
    public static final String WIKIMEDIA_RECENTCHANGE_TOPIC = "wikimedia.recentchange";
    private static Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());
    public static void main(String[] args) throws IOException {

        // create index in open search if it doesn't exist
        createWikimediaIndexIfAbsent();

        // create Kafka Client
        KafkaConsumer<String, String> consumer = setUpConsumer();
        consumeMessages(consumer);
    }

    private static void createWikimediaIndexIfAbsent() throws IOException {
        RestHighLevelClient openSearchClient = createOpenSearchClient();
        try(openSearchClient) {
            boolean indexExists = openSearchClient.indices().exists(new GetIndexRequest(INDEX_NAME), RequestOptions.DEFAULT);
            if (!indexExists) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest(INDEX_NAME);
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("The wikimedia index was created successfully!");
            } else {
                log.info("The " + INDEX_NAME +" index already exists!");
            }
        }
    }

    private static KafkaConsumer<String, String> setUpConsumer() {
        KafkaConsumer<String, String> consumer = createKafkaConsumer();
        consumer.subscribe(Collections.singleton(WIKIMEDIA_RECENTCHANGE_TOPIC));
        return consumer;
    }

    private static void consumeMessages(KafkaConsumer<String, String> consumer) throws IOException {
        RestHighLevelClient openSearchClient = createOpenSearchClient();
        try (consumer; openSearchClient) {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));
                logNumberOfRecords(records);
                BulkRequest bulkRequest = createBulkRequest(records);
                if (bulkRequest.numberOfActions() > 0) {
                    insertIntoOpenSearch(openSearchClient, bulkRequest);
                    commitOffsets(consumer);
                    waitForNewRecordsToQueue();
                }
            }
        }
    }

    private static void logNumberOfRecords(ConsumerRecords<String, String> records) {
        int recordCount = records.count();
        log.info("Received " + recordCount + " records");
    }

    private static BulkRequest createBulkRequest(ConsumerRecords<String, String> records) {
        BulkRequest bulkRequest = new BulkRequest();
        for (ConsumerRecord<String, String> record : records) {
            addRecordToBulkRequest(bulkRequest, record);
        }
        return bulkRequest;
    }

    private static void addRecordToBulkRequest(BulkRequest bulkRequest, ConsumerRecord<String, String> record) {
        String id = extractId(record.value());
        IndexRequest indexRequest = new IndexRequest(INDEX_NAME).source(record.value(), XContentType.JSON).id(id);
        bulkRequest.add(indexRequest);
    }

    private static String extractId(String value) {
        return JsonParser.parseString(value)
                .getAsJsonObject().get("meta")
                .getAsJsonObject().get("id")
                .getAsString();
    }

    private static void insertIntoOpenSearch(RestHighLevelClient openSearchClient, BulkRequest bulkRequest) throws IOException {
        BulkResponse response = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        log.info("Inserted " + response.getItems().length + " record(s).");
    }

    private static void commitOffsets(KafkaConsumer<String, String> consumer) {
        consumer.commitSync();
        log.info("Offsets have been committed");
    }
    private static void waitForNewRecordsToQueue() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static RestHighLevelClient createOpenSearchClient() {
        String connString = "http://localhost:9200";

        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);
        // extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


        }

        return restHighLevelClient;
    }



    private static KafkaConsumer<String, String> createKafkaConsumer() {
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS, LOCALHOST_BOOTSTRAP_SERVER);
        properties.setProperty(KEY_DESERIALIZER, StringDeserializer.class.getName());
        properties.setProperty(VALUE_DESERIALIZER, StringDeserializer.class.getName());
        properties.setProperty(GROUP_ID, DEFAULT_GROUP_ID);
        properties.setProperty(AUTO_OFFSET_RESET, AUTO_OFFSET_RESET_STRATEGY);
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return new KafkaConsumer<>(properties);
    }
}
