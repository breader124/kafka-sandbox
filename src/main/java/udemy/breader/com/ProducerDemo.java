package udemy.breader.com;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerDemo {
    public static void main(String[] args) {
        KafkaProducer<String, String> producer = KafkaSetup.setupProducer();

        ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "Hello World");

        producer.send(record);

        producer.close();
    }
}
