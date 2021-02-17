package udemy.breader.com.assignment.joins;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import udemy.breader.com.assignment.balance.Topic;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UserPurchaseJoinTest {

    private TopologyTestDriver testDriver;

    private TestInputTopic<String, String> userData;
    private TestInputTopic<String, String> userPurchase;
    private TestOutputTopic<String, String> innerJoinedDataPurchase;
    private TestOutputTopic<String, String> leftJoinedDataPurchase;

    @BeforeEach
    public void initEnvironment() {
        Properties properties = initProperties();
        Topology topology = UserPurchaseJoin.createTopology();
        testDriver = new TopologyTestDriver(topology, properties);
        initTestTopics();
    }

    public Properties initProperties() {
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "test:1234");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "user-purchase-test-app");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return properties;
    }

    public void initTestTopics() {
        StringSerializer stringSerializer = new StringSerializer();
        userData = testDriver.createInputTopic(Topic.USER_DATA.getTopicName(), stringSerializer, stringSerializer);
        userPurchase = testDriver.createInputTopic(Topic.USER_PURCHASE.getTopicName(), stringSerializer, stringSerializer);

        StringDeserializer stringDeserializer = new StringDeserializer();
        innerJoinedDataPurchase = testDriver.createOutputTopic(
                Topic.INNER_JOINED_DATA_PURCHASE.getTopicName(),
                stringDeserializer,
                stringDeserializer
        );
        leftJoinedDataPurchase = testDriver.createOutputTopic(
                Topic.LEFT_JOINED_DATA_PURCHASE.getTopicName(),
                stringDeserializer,
                stringDeserializer
        );
    }

    @AfterEach
    public void closeTopology() {
        testDriver.close();
    }

    @Test
    public void testSomething() {
        assertTrue(true);
    }
}