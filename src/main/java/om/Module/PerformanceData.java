/* This project is licensed under the Mulan PSL v2.
 You can use this software according to the terms and conditions of the Mulan PSL v2.
 You may obtain a copy of Mulan PSL v2 at:
     http://license.coscl.org.cn/MulanPSL2
 THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, EITHER EXPRESS OR
 IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR
 PURPOSE.
 See the Mulan PSL v2 for more details.
 Create: 2024
*/

package om.Module;

import CommonClass.CommonInterface;
import CommonClass.Parent;
import Utils.EsClientUtils2;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import om.constant.PerfConstant;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 性能数据采集.
 */
public class PerformanceData extends Parent implements CommonInterface {
    /**
     * 日志打印.
     */
    private static Logger logger = LoggerFactory.getLogger(PerformanceData.class);

    /**
     * 构造类.
     *
     * @throws IOException IO异常
     */
    public PerformanceData() throws IOException {
    }

    /**
     * json解析工具.
     */
    private static ObjectMapper objectMapper = new ObjectMapper();

    /**
     * kafka数据采集.
     */
    @Override
    public void run() {
        for (KafkaConsumer customer : this.KafkaConsumerList) {
            Runnable task = () -> {
                while (true) {
                    ConsumerRecords<String, String> poll = customer.poll(Duration.ofSeconds(2));
                    dealData(poll);
                    EsClientUtils2.getBulkProcess().flush();
                    customer.commitSync();
                }
            };
            this.thread_pool.execute(task);
        }
    }

    /**
     * 数据处理.
     *
     * @param records kafka数据
     * @return 数据处理列表
     */
    @Override
    public List<Map> dealData(ConsumerRecords<String, String> records) {
        ArrayList<Map> resutList = new ArrayList<>();
        for (ConsumerRecord<String, String> record : records) {
            String key = record.key();
            String value = record.value();
            String community = key;
            long offset = record.offset();
            int partition = record.partition();
            try {
                JsonNode dataNode = objectMapper.readTree(value);
                JsonNode perfNode = dataNode.get("data");
                if (perfNode == null || !perfNode.isArray()) {
                    continue;
                }
                Map<String, Map<String, Object>> perfDatas = new HashMap<>();
                community = community.toLowerCase();
                for (int i = 0; i < perfNode.size(); i++) {
                    Map<String, Map<String, Object>> dealDatas = dealDataNode(perfNode.get(i), offset, partition);
                    perfDatas.putAll(dealDatas);
                }
                String index = community + this.esIndex;
                for (String perfKey : perfDatas.keySet()) {
                    EsClientUtils2.insertOrUpdate(index, perfKey, perfDatas.get(perfKey));
                }
            } catch (Exception e) {
                logger.error(e.getMessage() + ":" + value);
            }
        }
        return resutList;
    }

    private Map<String, Map<String, Object>> dealDataNode(JsonNode node, long offset, int partition) {
        Map<String, Map<String, Object>> perfDatas = new HashMap<>();
        if (node == null) {
            return perfDatas;
        }
        Long timestamp = node.get("timestamp").asLong();
        Integer resType = node.get("res_type").asInt();
        String objectId = node.get("object_id").asText();
        String objectName = node.get("object_name").asText();
        JsonNode indicatorsNode = node.get("indicators");
        JsonNode indicatorValuesNode = node.get("indicator_values");
        JsonNode indicatorMaxValuesNode = node.get("indicator_max_values");
        if (StringUtils.isAllBlank(objectId, objectName)) {
            return perfDatas;
        }
        if (!PerfConstant.PERFORMANCE_METRIC.containsKey(resType)) {
            return perfDatas;
        }
        if (!indicatorsNode.isArray() || !indicatorMaxValuesNode.isArray()
                || !indicatorValuesNode.isArray()) {
            return perfDatas;
        }
        if (indicatorsNode.size() != indicatorValuesNode.size()
                || indicatorsNode.size() != indicatorMaxValuesNode.size()) {
            return perfDatas;
        }
        for (int j = 0; j < indicatorsNode.size(); j++) {
            Integer indicator = indicatorsNode.get(j).asInt();
            if (!PerfConstant.PERFORMANCE_METRIC.get(resType).contains(indicator)) {
                continue;
            }
            Map<String, Object> perfData = new HashMap<>();
            perfData.put("timestamp", dateFormat(timestamp));
            perfData.put("res_type", resType);
            perfData.put("object_id", objectId);
            perfData.put("object_name", objectName);
            perfData.put("indicator", indicator);
            perfData.put("value", indicatorValuesNode.get(j).asDouble());
            perfData.put("max_value", indicatorMaxValuesNode.get(j).asDouble());
            perfData.put("offset", offset);
            perfData.put("partition", partition);
            String perfKey = new StringBuilder().append(resType).append("_").append(indicator).append("_")
                    .append(objectName).toString();
            perfDatas.put(perfKey, perfData);
        }
        return perfDatas;
    }

    private String dateFormat(long timestamp) {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String format = dtf.format(LocalDateTime.ofInstant(Instant.ofEpochSecond(timestamp), ZoneId.of("+08:00")));
        return format.replace(" ", "T") + "+08:00";
    }
}
