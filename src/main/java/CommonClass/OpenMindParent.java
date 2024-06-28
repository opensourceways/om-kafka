package CommonClass;

import Utils.EsClientUtils2;
import Utils.PropertiesUtils;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.script.Script;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
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
    protected static ObjectMapper objectMapper = new ObjectMapper();
    protected ExecutorService thread_pool = Executors
            .newFixedThreadPool(Integer.parseInt(properties.get("thread_size").toString()));

    public OpenMindParent() throws IOException {
        String[] topics = properties.get("kafka.topic.name").toString().split(",");
        AdminClient adminClient = AdminClient.create(properties);
        for (String topic : topics) {
            try {
                DescribeTopicsResult describeTopicsResult = adminClient
                        .describeTopics(Collections.singletonList(topic));
                Map<String, KafkaFuture<TopicDescription>> futures = describeTopicsResult.values();
                KafkaFuture<TopicDescription> topicDescription = futures.get(topic);
                TopicDescription description = topicDescription.get();
                List<TopicPartitionInfo> partitions = description.partitions();
                List<TopicPartition> topicPartitions = new ArrayList<>();
                KafkaConsumer<String, String> kafkacustomer = new KafkaConsumer<String, String>(properties);
                for (TopicPartitionInfo partition : partitions) {
                    topicPartitions.add(new TopicPartition(topic, partition.partition()));
                    logger.info("topic: " + topic + ", partition: " + partition.partition());
                }
                kafkacustomer.assign(topicPartitions);
                for (TopicPartition tp : topicPartitions) {
                    kafkacustomer.seekToBeginning(Collections.singleton(tp));
                }
                KafkaConsumerList.add(kafkacustomer);

            } catch (Exception e) {
                logger.error(String.format("OpenMindParent init topic %s error", topic), e);
            }
        }
    }

    public void updateSatus(String event, String field, String termId) {
        TermQueryBuilder termQueryBuilder = new TermQueryBuilder(field, termId);
        BoolQueryBuilder must = QueryBuilders.boolQuery().must(termQueryBuilder);
        Script script = new Script("ctx._source['is_" + event + "'] = 1");
        try {
            EsClientUtils2.updateByQuery(this.esIndex, must, script);
        } catch (Exception e) {
            logger.error("updateSatus exception", e);
        }
    }

    public void updateSatusAll(List<Map> updateList) {
        for (Map resMap : updateList) {
            String event = resMap.get("event_type").toString();
            String termId = resMap.get("repo_id").toString();
            updateSatus(event, "repo_id.keyword", termId);
        }
    }

    public List<Map> eventAction(ConsumerRecords<String, String> records, String repoType) {
        List<Map> resutList = new ArrayList<>();
        for (ConsumerRecord<String, String> record : records) {
            String topic = record.topic();
            String value = record.value();
            try {
                HashMap<String, Object> resMap = objectMapper.readValue(value, HashMap.class);
                String body = resMap.get("Body").toString();
                byte[] decodedBytes = Base64.getDecoder().decode(body);
                String decodedString = new String(decodedBytes);
                resMap = objectMapper.readValue(decodedString, HashMap.class);
                resMap.put("offset", record.offset());
                resMap.put("partition", record.partition());
                String seconds = resMap.remove("time").toString();
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
                String create_at = sdf.format(new Date(Long.valueOf(seconds + "000")));
                resMap.put("created_at", create_at);
                resMap.put("event_type", topic);
                if (topic.contains(repoType + "_create")) {
                    resMap.put("repo_name", resMap.remove(repoType + "_name"));
                    resMap.put("repo_id", resMap.remove(repoType + "_id"));
                    resMap.put("is_repo_created", 1);
                } else if (topic.contains(repoType + "_updated") || topic.contains(repoType + "_deleted")) {
                    resMap.put("repo_name", resMap.remove(repoType + "_name"));
                    resMap.put("repo_id", resMap.remove(repoType + "_id"));
                    resutList.add(resMap);
                } else if (topic.contains("like") && !resMap.get("repo_type").equals(repoType)) {
                    continue;
                }
                String doc_id = resMap.get("repo_id").toString() + topic + create_at;
                EsClientUtils2.insertOrUpdate(this.esIndex, doc_id, resMap);
            } catch (Exception e) {
                logger.error(e.getMessage() + ":" + value, e);
            }
        }
        return resutList;
    }
}
