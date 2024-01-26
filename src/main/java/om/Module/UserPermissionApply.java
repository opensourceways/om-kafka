package om.Module;

import CommonClass.CommonInterface;
import CommonClass.Parent;
import Utils.EsClientUtils2;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class UserPermissionApply extends Parent implements CommonInterface {
    private static Logger logger = LogManager.getLogger(UserPermissionApply.class);

    public UserPermissionApply() throws IOException {
    }

    private static ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void run() {        
        for (KafkaConsumer customer : this.KafkaConsumerList) {
            String esIndex = this.esIndex;
            Runnable task = getTask(customer, esIndex);
            this.thread_pool.execute(task);
        }
    }

    public Runnable getTask(KafkaConsumer customer, String esIndex){      
        return () -> {
            while (true) {
                ConsumerRecords<String, String> poll = customer.poll(Duration.ofSeconds(2));
                List<Map> reList = dealData(poll);
                for (Map map : reList) {
                    putInfo(map, esIndex);
                }
                EsClientUtils2.getBulkProcess().flush();
                customer.commitSync();
            }
        };
    }

    public void putInfo(Map map, String esIndex){
        String id = (String) map.get("username");
        String community = (String) map.get("community");
        try {
            EsClientUtils2.insertOrUpdate(community + esIndex, id, map);
        } catch (IOException e) {
            try {
                logger.error(e + ":" + objectMapper.writeValueAsString(map), e);
            } catch (JsonProcessingException jsonProcessingException) {
                jsonProcessingException.printStackTrace();
                logger.error(jsonProcessingException + ":" + map, jsonProcessingException);
            }
        }
    }

    @Override
    public List<Map> dealData(ConsumerRecords<String, String> records) {
        ArrayList<Map> resutList = new ArrayList<>();
        for (ConsumerRecord<String, String> record : records) {
            String key = record.key();
            String value = record.value();
            String id = key;
            try {
                Map map = objectMapper.readValue(value, Map.class);
                map.put("offset", record.offset());
                map.put("partition", record.partition());
                String community = (String) map.get("community");
                community = community.toLowerCase();
                map.remove("community");

                EsClientUtils2.insertOrUpdate(community + this.esIndex, id, map);
            } catch (Exception e) {
                logger.error(e.getMessage() + ":" + value, e);
            }
        }
        return resutList;

    }
}
