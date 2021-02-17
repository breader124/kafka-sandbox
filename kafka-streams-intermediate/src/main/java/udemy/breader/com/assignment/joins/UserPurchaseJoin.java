package udemy.breader.com.assignment.joins;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import udemy.breader.com.assignment.balance.Topic;

import java.util.Properties;

public class UserPurchaseJoin {

    public static void main(String[] args) {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> purchaseStream = streamsBuilder.stream(Topic.USER_PURCHASE.getTopicName());
        GlobalKTable<String, String> userDataTable = streamsBuilder.globalTable(Topic.USER_DATA.getTopicName());

        purchaseStream.join(userDataTable, (key, value) -> key, (data, purchase) -> data + " -> " + purchase)
                .to(Topic.INNER_JOINED_DATA_PURCHASE.getTopicName());

        purchaseStream.leftJoin(userDataTable, (key, value) -> key, (data, purchase) -> data + " -> " + purchase)
                .to(Topic.LEFT_JOINED_DATA_PURCHASE.getTopicName());

        KafkaStreams kafkaStreamsApp = new KafkaStreams(streamsBuilder.build(), StreamsConf.getStreamsConfig());
        kafkaStreamsApp.start();

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreamsApp::close));
    }

    private static class StreamsConf {
        private static Properties getStreamsConfig() {
            Properties properties = new Properties();

            properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "user-data-join-app");
            properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
            properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
            properties.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            // only for development -- NOT FOR PROD configuration
            properties.setProperty(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

            return properties;
        }
    }
}
