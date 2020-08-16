package udemy.breader.com;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        KafkaProducer<String, String> producer = KafkaSetup.setupKafka();

        ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "Hello World");

        producer.send(record);

        producer.close();
    }
}
