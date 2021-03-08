package om.Module;

import CommonClass.CommonInterface;
import CommonClass.Parent;
import Utils.EsClientUtils2;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import om.QualityDashboard;
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

/**
 * @author xiazhonghai
 * @date 2021/3/3 17:03
 * @description:
 */
public class QualitySoftWare extends Parent implements CommonInterface {
    private static Logger logger = LogManager.getLogger(QualitySoftWare.class);
    public QualitySoftWare() throws IOException {
    }
    private static ObjectMapper objectMapper=new ObjectMapper();
    @Override
    public void run() {
        String esIndex=this.esIndex;
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
                                logger.error(e+":"+objectMapper.writeValueAsString(map),e);
                            } catch (JsonProcessingException jsonProcessingException) {
                                jsonProcessingException.printStackTrace();
                                logger.error(jsonProcessingException+":"+map,jsonProcessingException);
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
    public List<Map> dealData(ConsumerRecords<String,String> records) {
        ArrayList<Map> resutList = new ArrayList<>();
        for (ConsumerRecord<String, String> record : records) {
            String key = record.key();
            String value = record.value();
            try {
                Map map = objectMapper.readValue(value, Map.class);
                map.put("offset",record.offset());
                map.put("partition",record.partition());
                //获取id
                String id = (String)map.get("id");
                if(id==null||id.length()<=0){
                    id=key;
                }
                //获取创建时间
                Object access_control = map.get("access_control");
                if(access_control!=null){
                    Object job = ((Map) access_control).get("job");
                    if(job!=null){
                        Object ctime = ((Map) job).get("ctime")==null?"":((Map) job).get("ctime");
                        String create_time = ctime.toString().replace(" ", "T").concat("+08:00");
                        map.put("created_at",create_time);
                    }
                }
                map.put("id",id);
                resutList.add(map);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
                logger.error(e.getMessage()+":"+value,e);
            }
        }
        return resutList;

    }

}
