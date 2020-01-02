package kafka;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {
        public static RestHighLevelClient createClient(){
            //replace with your own credentials
            String hostname = "";
            String username = "";
            String password = "";
            // only for cloud
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username,password));

            RestClientBuilder builder =  RestClient.builder(
                    new HttpHost(hostname, 443, "https")).setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                @Override
                public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                    return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                }
            });

            RestHighLevelClient client = new RestHighLevelClient(builder);
            return client;
        }
        public static KafkaConsumer<String ,String> createConsumer(String topic){
            Properties properties = new Properties();
            String bootstrapServers = "127.0.0.1:9092";
            String groupId = "kafka-demo-elasticsearch";
            String topic1 = "twitter_tweet";
            //create consumer configs
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

            //create consumer
            KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

            //subscribe consumer to our topics
            consumer.subscribe(Arrays.asList(topic1));
            return consumer;
        }

    public static void main(String[] args) throws IOException, InterruptedException {
            Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);
            RestHighLevelClient client  = createClient();
//            String jsonString = "{\"name\":\"john\",\"age\":22,\"class\":\"mca\"}";

//            IndexRequest indexRequest =  new IndexRequest("twitter").source(jsonString, XContentType.JSON);
//            IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
//            String id  = indexResponse.getId();
//            logger.info(id);

            KafkaConsumer<String,String> consumer = createConsumer("twitter_tweet");

        //poll for new data
        while(true){
            ConsumerRecords<String , String > records = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String,String> record : records){
                // insert data into elastic search
                String jsonString = record.value();

                IndexRequest indexRequest =  new IndexRequest("twitter").source(jsonString, XContentType.JSON);
                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                String id  = indexResponse.getId();
                logger.info(id);
                Thread.sleep(1000);
            }
        }
            // close the client
//            client.close();
    }
    }
