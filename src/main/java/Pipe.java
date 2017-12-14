import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Pipe {

    private static final String SOURCE_STREAM_ID = "streams-plaintext-input";
    private static final String DESTINATION_STREAM_ID = "streams-pipe-output";

    private static final StreamsBuilder builder = new StreamsBuilder();
    private static KStream<String, String> sourceStream;
    private static KafkaStreams streams;

    private static final CountDownLatch latch = new CountDownLatch(1);

    public static void main(String[] args) {

        Properties kafkaConfig = new Properties();
        kafkaConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
        kafkaConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        kafkaConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        sourceStream = builder.stream(SOURCE_STREAM_ID);
        KTable<String, Long> count = sourceStream
                .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("|")))
                .groupBy((key, value) -> value)
                .count(Materialized.as("counts-store"));
        count.toStream().to(DESTINATION_STREAM_ID, Produced.with(Serdes.String(), Serdes.Long()));


        streams = new KafkaStreams(builder.build(), kafkaConfig);

        try {
            Runtime.getRuntime().addShutdownHook(new Thread("streams-control-c-hook") {
                @Override
                public void run() {
                    streams.close();
                    latch.countDown();
                }
            });
            streams.start();
            latch.await();
        } catch (Exception e) {
            System.exit(1);
        }
        System.exit(0);
    }

}
