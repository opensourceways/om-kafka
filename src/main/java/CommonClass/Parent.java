package CommonClass;

import Utils.PropertiesUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author xiazhonghai
 * @date 2021/3/3 17:04
 * @description:
 */
public class Parent extends Thread{
    protected  Properties properties= PropertiesUtils.readProperties();
    protected ArrayList<KafkaConsumer<String,String>>  KafkaConsumerList= new ArrayList<>();
    protected String esIndex=properties.getProperty("es.index");
    protected  ExecutorService thread_pool= Executors.newFixedThreadPool(Integer.parseInt(properties.get("thread_size").toString()));

    public Parent() throws IOException {
        String[] topics = properties.get("kafka.topic.name").toString().split(",");

        //设置kafka offset
        String topicOffset = properties.get("kafka.topic.offset").toString();
        //topic:offset,topic2:offset2
        String[] topciOffsets = topicOffset.split(",");
        for (String topciOffset : topciOffsets) {
            KafkaConsumer<String, String> kafkacustomer = new KafkaConsumer<String, String>(properties);
            String[] tf = topciOffset.split(":");
            TopicPartition topicPartition = new TopicPartition(topics[0], Integer.parseInt(tf[0]));
            kafkacustomer.assign(Arrays.asList(topicPartition));
            kafkacustomer.seek(topicPartition,Integer.parseInt(tf[1]));
            KafkaConsumerList.add(kafkacustomer);
        }
    }
}
