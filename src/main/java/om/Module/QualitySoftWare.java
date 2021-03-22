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
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.script.Script;
import org.joda.time.format.DateTimeFormat;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * @author xiazhonghai
 * @date 2021/3/3 17:03
 * @description:
 */
public class QualitySoftWare extends Parent implements CommonInterface {
    private static Logger logger = LogManager.getLogger(QualitySoftWare.class);
    public static Map<String, LocalDateTime> dateMap = new HashMap<>();
    public static final String accessKey = "access_key";
    public static final String commentKey = "comment_key";

    public QualitySoftWare() throws IOException {
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
                map.put("id", id);
                //获取创建时间
                Map access_control = (Map) map.get("access_control");
                Map spb = (Map) map.get("spb");
                Map comment = (Map) map.get("comment");

                //this is access_control iterm data
                if (access_control != null) {


                    Map job = (Map)((Map) access_control).get("job");
                    if (job != null) {
                        Object ctime = job.get("ctime") == null ? "" : job.get("ctime");
                        Object stime = job.get("stime");
                        Object etime = job.get("etime");
                        long startime = LocalDateTime.parse(stime.toString(), DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")).toEpochSecond(ZoneOffset.UTC);
                        long endtime = LocalDateTime.parse(etime.toString(), DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")).toEpochSecond(ZoneOffset.UTC);
                        map.put("access_control_total_time",endtime-startime);
                        //构建耗时
                        LocalDateTime date = dateMap.remove(id + commentKey);

                        LocalDateTime parse = LocalDateTime.parse(ctime.toString(), DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                        if (date == null) {
                            dateMap.put(id + accessKey, parse);
                        } else {

                            long buildElapse = date.toEpochSecond(ZoneOffset.UTC) - parse.toEpochSecond(ZoneOffset.UTC);
                            map.put("buildElapse", buildElapse);
                        }
                        String create_time = ctime.toString().replace(" ", "T").concat("+08:00");
                        map.put("created_at", create_time);
                    }
                    Map build = (Map) access_control.get("build");
                    if (build != null) {
                        List<Map> content = (List) build.get("content");
                        for (Map item : content) {
                            Object name = item.get("name");
                            Object result = item.get("result");
                            build.put(name, result);
                        }
                    }

                    map.put("is_access_control", 1);
                    // add access_conntrol result if success apply access_control.success to 1 else apply access_control.failed to 1
                    String result = (String) access_control.get("result");
                    if ("success".equals(result)) {
                        map.put("is_access_control_success", 1);
                    } else if ("failed".equals(result)) {
                        map.put("is_access_control_failed", 1);
                    }
                    map.put("is_latest_record_by_packagename", 1);
                    //update last build time according to package name
                    TermQueryBuilder termPackageQueryBuilder = new TermQueryBuilder("pull_request.package.keyword", ((Map) map.get("pull_request")).get("package"));
                    TermQueryBuilder termBranchQueryBuilder = new TermQueryBuilder("pull_request.target_branch.keyword", ((Map) map.get("pull_request")).get("target_branch"));
                    BoolQueryBuilder must = QueryBuilders.boolQuery().must(termBranchQueryBuilder).must(termPackageQueryBuilder);
                    Script script = new Script("ctx._source['is_latest_record_by_packagename']=0");
                    EsClientUtils2.updateByQuery(this.esIndex, must, script);
                }
                if (spb != null) {
                    Map job = (Map) spb.get("job");
                    String link = (String) job.get("link");
                    if (link.contains("x86-64")) {
                        map.put("is_x86_64", 1);
                        HashMap<Object, Object> tempMap = new HashMap<>();
                        Map build = (Map) spb.get("build");
                        if (build != null) {
                            String buildResult = (String) build.get("result");
                            if ("successful".equals(buildResult)) {
                                map.put("spb_build_success", 1);
                            } else {
                                map.put("spb_build_failed", 1);
                            }
                        }
                        //代码下载成功失败
                        Map scm = (Map) spb.get("scm");
                        if (scm != null) {
                            String scmResult = (String) scm.get("result");
                            if ("successful".equals(scmResult)) {
                                map.put("spb_scm_success", 1);
                            } else {
                                map.put("spb_scm_failed", 1);
                            }
                        }
                        tempMap.put("x86", map);
                        tempMap.put("offset", map.get("offset"));
                        tempMap.put("partition", map.get("partition"));
                        map = tempMap;
                    } else if (link.contains("aarch64")) {
                        map.put("is_aarch64", 1);
                        HashMap<Object, Object> tempMap = new HashMap<>();
                        Map build = (Map) spb.get("build");
                        if (build != null) {
                            String buildResult = (String) build.get("result");
                            if ("successful".equals(buildResult)) {
                                map.put("spb_build_success", 1);
                            } else {
                                map.put("spb_build_failed", 1);
                            }
                        }
                        Map scm = (Map) spb.get("scm");
                        if (scm != null) {
                            String scmResult = (String) scm.get("result");
                            if ("successful".equals(scmResult)) {
                                map.put("spb_scm_success", 1);
                            } else {
                                map.put("spb_scm_failed", 1);
                            }
                        }
                        tempMap.put("x64", map);
                        tempMap.put("offset", map.get("offset"));
                        tempMap.put("partition", map.get("partition"));
                        map = tempMap;
                    }

                }
                if (comment != null) {
                    //构建耗时
                    LocalDateTime date = dateMap.remove(id + accessKey);
                    Map build = (Map)comment.get("build");
                    String etime = (String) build.get("etime");
                    LocalDateTime etimeparse = LocalDateTime.parse(etime, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                    if (date == null) {
                        dateMap.put(id + commentKey, etimeparse);
                    } else {

                        long buildElapse = etimeparse.toEpochSecond(ZoneOffset.UTC) - date.toEpochSecond(ZoneOffset.UTC);
                        map.put("buildElapse", buildElapse);
                    }
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
