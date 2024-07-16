package om.Module;

import CommonClass.CommonInterface;
import CommonClass.Parent;
import Utils.EsClientUtils2;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CiResult extends Parent implements CommonInterface {
    private static Logger logger = LoggerFactory.getLogger(CiResult.class);
    public static Map<String, LocalDateTime> dateMap = new HashMap<>();
    public static final String accessKey = "access_key";
    public static final String commentKey = "comment_key";

    public CiResult() throws IOException {
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
                        String id = (String) map.get("id");
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
            String key = record.key();
            String value = record.value();
            String id = key;
            try {
                Map map = objectMapper.readValue(value, Map.class);
                map.put("offset", record.offset());
                map.put("partition", record.partition());

                String prUrl = map.get("pr_url").toString();
                String buildNum = map.get("build_no").toString();
                String updateAt = map.get("update_at").toString();
                String updateTime = dateFormat((long)Double.parseDouble(updateAt));

                if (map.containsKey("pr_create_at")) {
                    String prCreateAt = map.get("pr_create_at").toString();
                    String buildAt = map.get("build_at").toString();
                    map.put("pr_create_at", dateFormat((long)Double.parseDouble(prCreateAt)));
                    map.put("build_at", dateFormat((long)Double.parseDouble(buildAt)));
                    id = DigestUtils.md5Hex(prUrl + buildNum + updateTime);
                } else if (map.containsKey("commit_at")) {
                    String commitAt = map.get("commit_at").toString();
                    map.put("commit_at", dateFormat((long)Double.parseDouble(commitAt)));
                    id = DigestUtils.md5Hex(prUrl + buildNum + updateTime + key);
                }
                map.put("update_at", updateTime);

                EsClientUtils2.insertOrUpdate(this.esIndex, id, map);

            } catch (Exception e) {
                logger.error(e.getMessage() + ":" + value, e);
            }
        }
        return resutList;

    }

    private String dateFormat(long timestamp) {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String format = dtf.format(LocalDateTime.ofInstant(Instant.ofEpochSecond(timestamp), ZoneId.of("+08:00")));
        return format.replace(" ", "T") + "+08:00";
    }
}
