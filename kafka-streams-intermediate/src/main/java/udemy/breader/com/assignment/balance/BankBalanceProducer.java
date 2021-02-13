package udemy.breader.com.assignment.balance;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class BankBalanceProducer {

    private static final Random r = new Random();
    private static final List<String> customerList = Arrays.asList("John", "Jane", "Anna", "Marius", "Adrian", "Agata");

    public static void main(String[] args) {
        Properties properties = BalanceProducerConfig.getProducerProperties();
        String topicName = BalanceProducerConfig.getTopicName();
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        Gson gson = new Gson();

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(() -> {
            Transaction transaction = new Transaction(nextRandomCustomer(), nextRandomAmount(), getUtcTime());
            String transactionJson = gson.toJson(transaction);
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, transactionJson);
            producer.send(record);
        }, 0, 6, TimeUnit.MILLISECONDS);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            scheduler.shutdown();
            producer.close();
        }));
    }

    public static String getUtcTime() {
        DateTimeFormatter formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        return now.format(formatter);
    }

    public static String nextRandomCustomer() {
        return customerList.get(r.nextInt(customerList.size()));
    }

    public static Integer nextRandomAmount() {
        return r.nextInt(200);
    }
}
