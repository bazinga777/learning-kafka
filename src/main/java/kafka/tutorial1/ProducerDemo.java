package kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        //create producer properties
        String jsonString = "{\"name\":\"john\",\"age\":22,\"class\":\"mca\"}";
        Properties properties = new Properties();
        String bootstrapServers = "127.0.0.1:9092";
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create the producer
        KafkaProducer<String ,String> producer = new KafkaProducer<String, String>(properties);

        //Create producer record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("twitter_tweet", jsonString);

        //send the data -asynchronous
        producer.send(record);

        //flush data
        producer.flush();

        //flush and close producer
        producer.close();

    }
}
