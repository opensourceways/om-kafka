package om;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.*;

/**
 * @author xiazhonghai
 * @date 2021/1/12 16:41
 * @description:
 */
public class QualityDashboard {
    private static Logger logger = LogManager.getLogger(QualityDashboard.class);
    static Properties conf = new Properties();
    static KafkaConsumer<String, String> kafkacustomer;
    public static RestHighLevelClient client;

    static {
        InputStream resourceAsStream = QualityDashboard.class.getClassLoader().getResourceAsStream("conf.properties");
        try {
            conf.load(resourceAsStream);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        conf.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        conf.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkacustomer = new KafkaConsumer<String, String>(conf);
        kafkacustomer.subscribe(Arrays.asList(conf.get("obs.build.topic").toString().split(",")));

    }

    public static void main(String[] args) throws JsonProcessingException, InterruptedException {
        HashMap<String, String> offsetMap = new HashMap<>();
        List<String> strings = Arrays.asList(args);
        for (String string : strings) {
            String[] split = string.split("-");
            offsetMap.put(split[0], split[1]);

        }


        ArrayList<Customer> customerThreads = new ArrayList();
        for (Map.Entry<String, String> stringStringEntry : offsetMap.entrySet()) {
            String partitionnum = stringStringEntry.getKey();
            String offset = stringStringEntry.getValue();
            TopicPartition topicPartition = new TopicPartition(conf.getProperty("obs.build.topic").toString(), Integer.parseInt(partitionnum));
            Customer customer = new Customer(topicPartition, conf, topicPartition.toString(), Integer.parseInt(offset),LogManager.getLogger("offset"+partitionnum));
            customer.start();
            customerThreads.add(customer);

        }
        for (Customer customerThread : customerThreads) {
            customerThread.join();
        }

    }

    /***
     * 功能描述:reset kafka offset
     * @param offset: offset position
     * @return: void
     * @Author: xiazhonghai
     * @Date: 2021/1/13 11:26
     */
    public static void resetOffset(Map<String, String> offset) {
        Set<TopicPartition> partitionset;
        kafkacustomer.poll(Duration.ofSeconds(20));
        partitionset = kafkacustomer.assignment();

        for (TopicPartition partition : partitionset) {
            for (Map.Entry<String, String> stringStringEntry : offset.entrySet()) {
                if (partition.partition() == Integer.parseInt(stringStringEntry.getKey())) {
                    kafkacustomer.seek(partition, Integer.parseInt(stringStringEntry.getValue()));
                }
            }
        }
    }
}
