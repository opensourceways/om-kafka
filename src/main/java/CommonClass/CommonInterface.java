package CommonClass;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.List;

/**
 * @author xiazhonghai
 * @date 2021/3/3 17:54
 * @description:
 */
public interface CommonInterface {
    List dealData(ConsumerRecords<String,String> records);
}
