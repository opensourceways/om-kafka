package CommonClass;

import Utils.EsClientUtils2;
import Utils.PropertiesUtils;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.script.Script;

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

    public void updateSatus(String topic, String field, String termId) {
        TermQueryBuilder termQueryBuilder = new TermQueryBuilder(field, termId);
        BoolQueryBuilder must = QueryBuilders.boolQuery().must(termQueryBuilder);
        Script script = new Script("ctx._source['is_" + topic + "'] = 1");
        try {
            EsClientUtils2.updateByQuery(this.esIndex, must, script);
        } catch (Exception e) {
            logger.error("updateSatus exception", e);
        }
    }

    public void updateSatusAll(List<Map> updateList) {
        for (Map resMap : updateList) {
            String topic = resMap.get("event_type").toString();
            String field = topic.split("_")[0];
            String fieldName = field + "_id";
            String repoId = resMap.get(fieldName).toString();
            updateSatus(topic, fieldName + ".keyword", repoId);
        }
    }
}
