package avro;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaAvroProducer {

    private static final String SCHEMA_REGISTRY_URL = "schema.registry.url";
    private static final String TOPIC = "consumer";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:9092");
        properties.put(ProducerConfig.ACKS_CONFIG, "0");
        properties.put(ProducerConfig.RETRIES_CONFIG, "10");

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.put(SCHEMA_REGISTRY_URL, "http://127.0.0.1:8081");

        KafkaProducer<String, Customer> kafkaProducer = new KafkaProducer<>(properties);

        Customer customer = Customer.newBuilder()
                .setFirstName("Tomasz")
                .setLastName("Galuszka")
                .setAge(26)
                .setHeight(174)
                .setWidth(32)
                .setAutomatedEmail(false)
                .build();

        ProducerRecord<String, Customer> producerRecord = new ProducerRecord<>(TOPIC, "test", customer);

        kafkaProducer.send(producerRecord, (recordMetadata, e) -> {
            if (e == null) {
                System.out.println("Success");
                System.out.printf(recordMetadata.toString());
            } else {
                e.printStackTrace();
            }
        });

        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
