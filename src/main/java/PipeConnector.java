import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;


public class PipeConnector {

    public static void main(String[] args) throws InterruptedException {

        Properties kafkaConfig = new Properties();
        kafkaConfig.put("bootstrap.servers", "localhost:9092");
        kafkaConfig.put("group.id", "pipe.reader");
        kafkaConfig.put("schema.registry.url", "http://localhost:8081");
        kafkaConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);

        String topic = "topic3";

        KafkaConsumer consumer = new KafkaConsumer<>(kafkaConfig);
        consumer.subscribe(Collections.singletonList(topic));



        while (true) {
            ConsumerRecords records = consumer.poll(1000);
            records.forEach(r -> System.out.println("aa"+ r));
        }
    }
}
