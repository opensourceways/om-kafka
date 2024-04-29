package CommonClass;

import Utils.PropertiesUtils;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class OpenMindParent extends Thread {
    private static Logger logger = LogManager.getLogger(OpenMindParent.class);
    protected Properties properties = PropertiesUtils.readProperties();
    protected ArrayList<KafkaConsumer<String, String>> KafkaConsumerList = new ArrayList<>();
    protected String esIndex = properties.getProperty("es.index");
    protected ExecutorService thread_pool = Executors
            .newFixedThreadPool(Integer.parseInt(properties.get("thread_size").toString()));

    public OpenMindParent() throws IOException {
        String[] topics = properties.get("kafka.topic.name").toString().split(",");

        try {
            AdminClient adminClient = AdminClient.create(properties);
            DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Arrays.asList(topics));
            Map<String, TopicDescription> topicDescriptionMap = describeTopicsResult.all().get();
            KafkaConsumer<String, String> kafkacustomer = new KafkaConsumer<String, String>(properties);
            List<TopicPartition> topicPartitions = new ArrayList<>();
            for (String topic : topics) {
                TopicDescription topicDescription = topicDescriptionMap.get(topic);
                for (TopicPartitionInfo partitionInfo : topicDescription.partitions()) {
                    topicPartitions.add(new TopicPartition(topic, partitionInfo.partition()));
                    logger.info("topic: " + topic + ", partition: " + partitionInfo.partition());
                }
            }
            kafkacustomer.assign(topicPartitions);

            for (TopicPartition tp : topicPartitions) {
                kafkacustomer.seekToBeginning(Collections.singleton(tp));
            }
            KafkaConsumerList.add(kafkacustomer);

        } catch (Exception e) {
            logger.error("OpenMindParent init exception", e);
        }
    }
}
