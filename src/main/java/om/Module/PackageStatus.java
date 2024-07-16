package om.Module;

import CommonClass.CommonInterface;
import CommonClass.Parent;
import Utils.EsClientUtils2;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author zhw
 * @date 2021/7/13 14:52
 * @description: add {'Software package build duration':'archive_end-archive_start','wait time':'iso_start-archive_end',
 * 'make ios time':'iso_end-iso_start','build tiome':'iso_end-archive_start'}
 */
public class PackageStatus extends Parent implements CommonInterface {

    private static Logger logger = LoggerFactory.getLogger(PackageStatus.class);
    public static Map<String, LocalDateTime> dateMap = new HashMap<>();
    public static final String accessKey = "access_key";
    public static final String commentKey = "comment_key";

    public PackageStatus() throws IOException {
    }

    public void QualitySoftWare() throws IOException {
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

            try {
                Map map = objectMapper.readValue(value, Map.class);
                map.put("offset", record.offset());
                map.put("partition", record.partition());

                //获取id much datas has no id in record's in kafka this record's key equals to one record's id which has id field
                String id = (String) map.get("id");
                if (id == null || id.length() <= 0) {
                    id = key;
                }

                try {
                    String time_stamp = (String) map.get("time_stamp");
                    String create_time = time_stamp.replace(" ", "T").concat("+08:00");
                    map.put("created_at", create_time);

                    String obs_project = (String) map.get("obs_project");
                    String hostarch = (String) map.get("hostarch");
                    map.put("hostarch", hostarch.replace("standard_", ""));

                    id = obs_project + hostarch + create_time;
                } catch (Exception e) {
                    continue;
                }

                EsClientUtils2.insertOrUpdate(this.esIndex, id, map);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
                logger.error(e.getMessage() + ":" + value, e);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return resutList;

    }


}
