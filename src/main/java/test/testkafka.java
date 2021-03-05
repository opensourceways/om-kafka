package test;

import om.Customer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;

import java.util.Properties;

/**
 * @author xiazhonghai
 * @date 2021/2/9 10:14
 * @description:
 */
public class testkafka {
    public static void main(String[] args) {
        TopicPartition topicPartition = new TopicPartition("openeuler_statewall_ci_ac", 0);
        Properties properties = new Properties();

    }
}
