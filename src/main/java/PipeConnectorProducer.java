import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class PipeConnectorProducer {

    public static void main(String[] args) {

        Properties kafkaConfig = new Properties();
        kafkaConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        kafkaConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        kafkaConfig.put("schema.registry.url", "http://localhost:8081");

        KafkaProducer kafkaProducer = new KafkaProducer<>(kafkaConfig);

        String key = "k1";
        String userSchema = "{" +
                "\"type\":\"record\"," +
                "\"name\":\"Customer\"," +
                "\"fields\":[" +
                    "{\"name\": \"id\", \"type\": \"int\"}," +
                    "{\"name\": \"name\", \"type\": \"string\"}" +
                "]" +
                "}";

        GenericData.Record customer = new GenericData.Record(new Schema.Parser().parse(userSchema));
        customer.put("id", 12);
        customer.put("name", "TomaszGaluszka");

        ProducerRecord<String, GenericData.Record> record = new ProducerRecord<>("topic3", key, customer);
        try {
            kafkaProducer.send(record);
        } catch (Exception e) {
            System.out.println(e.toString());
        }

    }
}
