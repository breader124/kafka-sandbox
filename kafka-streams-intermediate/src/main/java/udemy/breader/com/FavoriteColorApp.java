package udemy.breader.com;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class FavoriteColorApp {

    public static void main(String[] args) {
        Properties appProperties = kafkaProperties();

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> inputFirstStream = builder.stream("fav-color-input");
        inputFirstStream.filter(FavoriteColorApp::isValidColor).to("fav-color-inter");

        KTable<String, String> inputIntermediaryStream = builder.table("fav-color-inter");
        KTable<String, Long> stats = inputIntermediaryStream
                .groupBy((key, value) -> KeyValue.pair(value, key))
                .count();
        KStream<String, Long> outputStream = stats.toStream();
        outputStream.to("fav-color-output", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams app = new KafkaStreams(builder.build(), appProperties);
        app.start();

        Runtime.getRuntime().addShutdownHook(new Thread(app::close));
    }

    public static Properties kafkaProperties() {
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "fav-color");
        kafkaProperties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaProperties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        kafkaProperties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        kafkaProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return kafkaProperties;
    }

    public static boolean isValidColor(String user, String color) {
        return "green".equals(color) || "red".equals(color) || "blue".equals(color);
    }
}
