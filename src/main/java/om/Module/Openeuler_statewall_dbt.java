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
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.script.Script;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author zhw
 * @date 2021/7/13 14:52
 * @description: add {'Software package build duration':'archive_end-archive_start','wait time':'iso_start-archive_end',
 *                    'make ios time':'iso_end-iso_start','build tiome':'iso_end-archive_start'}
 */
public class Openeuler_statewall_dbt extends Parent implements CommonInterface {

    private static Logger logger = LogManager.getLogger(QualitySoftWare.class);
    public static Map<String, LocalDateTime> dateMap = new HashMap<>();
    public static final String accessKey = "access_key";
    public static final String commentKey = "comment_key";

    public Openeuler_statewall_dbt() throws IOException {
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
                //map.put("id", id);
                //获取创建时间
                String archive_start =  (String)map.get("archive_start");
                String archive_end = (String)map.get("archive_end");
                String iso_start = (String) map.get("iso_start");
                String iso_end = (String) map.get("iso_end");


                Pattern a= Pattern.compile("^((\\d{2}(([02468][048])|([13579][26]))[\\-\\/\\s]?((((0?[13578])|(1[02]))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(3[01])))|(((0?[469])|(11))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(30)))|(0?2[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])))))|(\\d{2}(([02468][1235679])|([13579][01345789]))[\\-\\/\\s]?((((0?[13578])|(1[02]))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(3[01])))|(((0?[469])|(11))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(30)))|(0?2[\\-\\/\\s]?((0?[1-9])|(1[0-9])|(2[0-8]))))))(\\s((([0-1][0-9])|(2?[0-3]))\\:([0-5]?[0-9])((\\s)|(\\:([0-5]?[0-9])))))?$");
                Matcher a1=a.matcher(archive_start);

                Pattern b= Pattern.compile("^((\\d{2}(([02468][048])|([13579][26]))[\\-\\/\\s]?((((0?[13578])|(1[02]))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(3[01])))|(((0?[469])|(11))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(30)))|(0?2[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])))))|(\\d{2}(([02468][1235679])|([13579][01345789]))[\\-\\/\\s]?((((0?[13578])|(1[02]))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(3[01])))|(((0?[469])|(11))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(30)))|(0?2[\\-\\/\\s]?((0?[1-9])|(1[0-9])|(2[0-8]))))))(\\s((([0-1][0-9])|(2?[0-3]))\\:([0-5]?[0-9])((\\s)|(\\:([0-5]?[0-9])))))?$");
                Matcher b1=b.matcher(archive_end);

                Pattern c= Pattern.compile("^((\\d{2}(([02468][048])|([13579][26]))[\\-\\/\\s]?((((0?[13578])|(1[02]))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(3[01])))|(((0?[469])|(11))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(30)))|(0?2[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])))))|(\\d{2}(([02468][1235679])|([13579][01345789]))[\\-\\/\\s]?((((0?[13578])|(1[02]))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(3[01])))|(((0?[469])|(11))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(30)))|(0?2[\\-\\/\\s]?((0?[1-9])|(1[0-9])|(2[0-8]))))))(\\s((([0-1][0-9])|(2?[0-3]))\\:([0-5]?[0-9])((\\s)|(\\:([0-5]?[0-9])))))?$");
                Matcher c1=c.matcher(iso_start);

                Pattern d= Pattern.compile("^((\\d{2}(([02468][048])|([13579][26]))[\\-\\/\\s]?((((0?[13578])|(1[02]))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(3[01])))|(((0?[469])|(11))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(30)))|(0?2[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])))))|(\\d{2}(([02468][1235679])|([13579][01345789]))[\\-\\/\\s]?((((0?[13578])|(1[02]))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(3[01])))|(((0?[469])|(11))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(30)))|(0?2[\\-\\/\\s]?((0?[1-9])|(1[0-9])|(2[0-8]))))))(\\s((([0-1][0-9])|(2?[0-3]))\\:([0-5]?[0-9])((\\s)|(\\:([0-5]?[0-9])))))?$");
                Matcher d1=d.matcher(iso_end);
                if(a1.matches() && b1.matches() && c1.matches() && d1.matches()) {
                    long archive_start_long = LocalDateTime.parse(archive_start.toString(), DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")).toEpochSecond(ZoneOffset.of("+8"));
                    long archive_end_long = LocalDateTime.parse(archive_end.toString(), DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")).toEpochSecond(ZoneOffset.of("+8"));
                    long iso_start_long = LocalDateTime.parse(iso_start.toString(), DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")).toEpochSecond(ZoneOffset.of("+8"));
                    long iso_end_long = LocalDateTime.parse(iso_end.toString(), DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")).toEpochSecond(ZoneOffset.of("+8"));
                    //添加字段
                    map.put("software_package_build_duration", archive_end_long - archive_start_long);
                    map.put("waiting_time", iso_start_long - archive_end_long);
                    map.put("make_ios_time", iso_end_long - iso_start_long);
                    map.put("build_version_time", iso_end_long - archive_start_long);

                }

                //数值类型转化可以做差

                //this is access_control iterm data

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
