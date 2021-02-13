package udemy.breader.com.assignment.balance;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class BalanceProducerConfig {
    public static Properties getProducerProperties() {
        Properties p = new Properties();

        // general
        p.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        p.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // idempotence
        p.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        p.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        p.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        p.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        // compressing
        p.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        p.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");

        return p;
    }

    public static String getTopicName() {
        return "bank-balance-input";
    }
}
