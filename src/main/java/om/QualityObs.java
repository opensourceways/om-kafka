package om;

import Utils.PropertiesUtils;
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
public class QualityObs extends Thread {
    private static Logger logger = LogManager.getLogger(QualityObs.class);
    static Properties conf;
    static KafkaConsumer<String, String> kafkacustomer;
    public static RestHighLevelClient client;

    static {
        try {
            conf = PropertiesUtils.readProperties();
        } catch (IOException e) {
            e.printStackTrace();
        }
        conf.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        conf.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkacustomer = new KafkaConsumer<String, String>(conf);
        kafkacustomer.subscribe(Arrays.asList(conf.get("kafka.topic.name").toString().split(",")));

    }


    @Override
    public void run() {

        HashMap<String, String> offsetMap = new HashMap<>();
        String offsetStr = conf.getProperty("kafka.topic.offset");
        String[] offsets = offsetStr.split(",");
        for (String string : offsets) {
            String[] split = string.split(":");
            offsetMap.put(split[0], split[1]);

        }


        ArrayList<Customer> customerThreads = new ArrayList();
        for (Map.Entry<String, String> stringStringEntry : offsetMap.entrySet()) {
            String partitionnum = stringStringEntry.getKey();
            String offset = stringStringEntry.getValue();
            TopicPartition topicPartition = new TopicPartition(conf.getProperty("kafka.topic.name").toString(), Integer.parseInt(partitionnum));
            Customer customer = new Customer(topicPartition, conf, topicPartition.toString(), Integer.parseInt(offset),LogManager.getLogger("offset"+partitionnum));
            customer.start();
            customerThreads.add(customer);

        }
        for (Customer customerThread : customerThreads) {
            try {
                customerThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
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
