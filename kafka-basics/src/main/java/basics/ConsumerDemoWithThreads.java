package basics;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreads {
    public static void main(String[] args) throws InterruptedException {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class.getName());

        String groupId = "fifth-application";
        String topic = "first_topic";

        CountDownLatch latch = new CountDownLatch(1);
        ConsumerRunnable runnable = new ConsumerRunnable(groupId, topic, latch);

        Thread consumerThread = new Thread(runnable);
        consumerThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            runnable.shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                logger.info("On shutdown hook");
            }
        }));

        latch.await();
    }

    private static class ConsumerRunnable implements Runnable {
        private final Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

        private final CountDownLatch latch;
        private final KafkaConsumer<String, String> consumer;

        public ConsumerRunnable(String groupId, String topic, CountDownLatch latch) {
            this.latch = latch;

            this.consumer = KafkaSetup.setupConsumer(groupId);
            consumer.subscribe(Collections.singleton(topic));
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key: " + record.key() + ", Value: " + record.value());
                        logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                    }
                }
            } catch (WakeupException exc) {
                logger.info("Caught WakeUpException");
            } finally {
                consumer.close();
                latch.countDown();
            }
        }

        public void shutdown() {
            logger.info("Shutdown called");
            consumer.wakeup();
        }
    }
}
