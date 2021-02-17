package udemy.breader.com.assignment.joins;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import udemy.breader.com.assignment.balance.Topic;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Producer {

    private static final List<String> usernameList = Arrays.asList("Adrian", "Agata", "Bartek", "Marek", "Nikolai");
    private static final List<String> cityList = Arrays.asList("Warsaw", "Madrid", "Munich", "Barcelona", "Moscow");

    private static final KafkaProducer<String, String> producer = new KafkaProducer<>(ProducerConf.getProducerConfig());
    private static final Random r = new Random();

    public static void main(String[] args) {

        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            sendUserDataRecord();
            sendUserPurchaseRecord();
        }, 0, 1, TimeUnit.SECONDS);

        Runtime.getRuntime().addShutdownHook(new Thread(producer::close));
    }

    private static void sendUserDataRecord() {
        String username = getRandomListElement(usernameList);
        String city = r.nextBoolean() ? getRandomListElement(cityList) : null;
        ProducerRecord<String, String> record = new ProducerRecord<>(Topic.USER_DATA.getTopicName(), username, city);
        producer.send(record);
    }

    private static void sendUserPurchaseRecord() {
        String username = getRandomListElement(usernameList);
        String purchase = Integer.toString(r.nextInt());
        ProducerRecord<String, String> record = new ProducerRecord<>(Topic.USER_PURCHASE.getTopicName(), username, purchase);
        producer.send(record);
    }

    private static String getRandomListElement(List<String> elementList) {
        return elementList.get(r.nextInt(elementList.size()));
    }

    private static class ProducerConf {
        private static Properties getProducerConfig() {
            Properties properties = new Properties();

            properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
            properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
            properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
            properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));

            properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
            properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");

            return properties;
        }
    }
}
