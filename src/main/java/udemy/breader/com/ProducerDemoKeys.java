package udemy.breader.com;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        KafkaProducer<String, String> producer = KafkaSetup.setupProducer();

        String topic = "first_topic";
        for (int i = 0; i < 10; i++) {
            String value = "Hello World for " + i + " time";

            String key = "id_" + i;
            logger.info("Key: " + key);

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            producer.send(record, (recordMetadata, e) -> {
                if (e == null) {
                    logger.info("Received new metadata\n" +
                            "Topic: " + recordMetadata.topic() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "Timestamp: " + recordMetadata.timestamp());
                } else {
                    logger.error("Error while producing", e);
                }
            }).get();
            // get here only for demonstration purpose
        }

        producer.close();
    }
}
