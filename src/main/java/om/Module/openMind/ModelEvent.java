package om.Module.openMind;

import CommonClass.CommonInterface;
import CommonClass.OpenMindParent;
import Utils.EsClientUtils2;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ModelEvent extends OpenMindParent implements CommonInterface {
    private static Logger logger = LogManager.getLogger(ModelEvent.class);

    public ModelEvent() throws IOException {
    }

    private static ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void run() {
        for (KafkaConsumer customer : this.KafkaConsumerList) {
            Runnable task = () -> {
                while (true) {
                    ConsumerRecords<String, String> poll = customer.poll(Duration.ofSeconds(2));
                    List<Map> updateList = dealData(poll);
                    EsClientUtils2.getBulkProcess().flush();
                    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
                    scheduler.schedule(() -> updateSatusAll(updateList), 10, TimeUnit.SECONDS);
                    customer.commitSync();
                }
            };
            this.thread_pool.execute(task);
        }
    }

    // {"time":1714271011,"owner":"modelfoundryinfra","model_id":"6766","created_by":"xiyuanwang"}
    // {"model_id":"6519","deleted_by":"guoxiaozhen"}
    // {"time":1714272469,"repo":"model_14272463507","owner":"guoxiaozhen","model_id":"20162","updated_by":"guoxiaozhen","is_pri_to_pub":false}
    @Override
    public List<Map> dealData(ConsumerRecords<String, String> records) {
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
                String seconds = resMap.get("time").toString();
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
                String create_at = sdf.format(new Date(Long.valueOf(seconds + "000")));
                resMap.put("created_at", create_at);
                resMap.put("event_type", topic);
                if (topic.equals("model_created")) {
                    resMap.put("is_model_created", 1);
                } else if (topic.equals("model_updated") || topic.equals("model_deleted")) {
                    resMap.put("topic", topic);
                    resutList.add(resMap);
                } else if (topic.equals("like_create") || topic.equals("like_delete")) {
                    if (resMap.get("repo_type").equals("model") && resMap.containsKey("repo_id")) {
                        resMap.put("model_id", resMap.get("repo_id"));
                    }
                }
                String doc_id = resMap.get("model_id").toString() + topic + create_at;
                EsClientUtils2.insertOrUpdate(this.esIndex, doc_id, resMap);
            } catch (Exception e) {
                logger.error(e.getMessage() + ":" + value, e);
            }
        }
        return resutList;

    }
}
