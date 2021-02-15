package udemy.breader.com.assignment.balance;

import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;

public class BankBalanceStreams {

    private static final String amountIntermediary = "bank-balance-amount-intermediary";
    private static final String amountOutput = "bank-balance-amount-output";

    private static final String timeIntermediary = "bank-balance-time-intermediary";
    private static final String timeOutput = "bank-balance-time-output";


    public static void main(String[] args) {
        Properties properties = BalanceStreamsConfig.getStreamsConfig();

        StreamsBuilder builder = new StreamsBuilder();
        Gson gson = new Gson();

        KStream<String, String> inputStream = builder.stream(BalanceProducerConfig.getTopicName());
        inputStream
                .map((key, value) -> {
                    Transaction t = gson.fromJson(value, Transaction.class);
                    return KeyValue.pair(t.creditor, t.amount);
                })
                .to(amountIntermediary, Produced.with(Serdes.String(), Serdes.Integer()));
        inputStream
                .map((key, value) -> {
                    Transaction t = gson.fromJson(value, Transaction.class);
                    return KeyValue.pair(t.creditor, t.isoTime);
                })
                .to(timeIntermediary); // log compacted topic

        KStream<String, Integer> amountInputStream = builder.stream(amountIntermediary, Consumed.with(Serdes.String(), Serdes.Integer()));
        amountInputStream
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()))
                .reduce(Integer::sum, Named.as("Balance"), Materialized.with(Serdes.String(), Serdes.Integer()))
                .toStream()
                .to(amountOutput, Produced.with(Serdes.String(), Serdes.Integer()));

        KTable<String, String> timeInputTable = builder.table(timeIntermediary);
        timeInputTable.toStream().to(timeOutput);

        KafkaStreams streamsApp = new KafkaStreams(builder.build(), properties);
        streamsApp.cleanUp();
        streamsApp.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streamsApp::close));
    }
}
