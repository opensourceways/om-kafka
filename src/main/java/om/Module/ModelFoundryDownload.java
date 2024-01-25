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
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ModelFoundryDownload extends Parent implements CommonInterface {
    private static Logger logger = LogManager.getLogger(ModelFoundryDownload.class);

    public ModelFoundryDownload() throws IOException {
    }

    private static ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void run() {
        String esIndex = this.esIndex;
        for (KafkaConsumer customer : this.KafkaConsumerList) {
            Runnable task = () -> {
                while (true) {
                    ConsumerRecords<String, String> poll = customer.poll(Duration.ofSeconds(2));
                    List<Map> reList = dealData(poll);
                    for (Map map : reList) {
                        String id = (String) map.get("request_ID");
                        try {
                            EsClientUtils2.insertOrUpdate(esIndex, id, map);
                        } catch (IOException e) {
                            try {
                                logger.error(e + ":" + objectMapper.writeValueAsString(map), e);
                            } catch (JsonProcessingException jsonProcessingException) {
                                jsonProcessingException.printStackTrace();
                                logger.error(jsonProcessingException + ":" + map, jsonProcessingException);
                            }
                        }
                    }
                    EsClientUtils2.getBulkProcess().flush();
                    customer.commitSync();
                }
            };
            this.thread_pool.execute(task);
        }
    }

    @Override
    public List<Map> dealData(ConsumerRecords<String, String> records) {
        ArrayList<Map> resutList = new ArrayList<>();
        for (ConsumerRecord<String, String> record : records) {
            String value = record.value();
            try {
                HashMap<String, Object> resMap = objectMapper.readValue(value, HashMap.class);
                String body = resMap.get("Body").toString();
                byte[] decodedBytes = Base64.getDecoder().decode(body);
                String decodedString = new String(decodedBytes);
                resMap = objectMapper.readValue(decodedString, HashMap.class);
                resMap.put("offset", record.offset());
                resMap.put("partition", record.partition());
                HashMap<String, Object> details = (HashMap<String, Object>) resMap.remove("details");
                for (String details_key : details.keySet()) {
                    resMap.put(details_key, details.get(details_key));            
                }
                String seconds = resMap.get("created_at").toString();
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");  
                String create_at = sdf.format(new Date(Long.valueOf(seconds+"000")));
                resMap.put("created_at", create_at);              

                String doc_id = resMap.get("request_ID").toString();
                EsClientUtils2.insertOrUpdate(this.esIndex, doc_id, resMap);
            } catch (Exception e) {
                logger.error(e.getMessage() + ":" + value, e);
            }
        }
        return resutList;

    }
}
