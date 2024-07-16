package om.Module.openMind;

import CommonClass.CommonInterface;
import CommonClass.OpenMindParent;
import Utils.EsClientUtils2;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DatasetEvent extends OpenMindParent implements CommonInterface {
    private static Logger logger = LoggerFactory.getLogger(DatasetEvent.class);
    private ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public DatasetEvent() throws IOException {
    }


    @Override
    public void run() {
        for (KafkaConsumer customer : this.KafkaConsumerList) {
            Runnable task = () -> {
                while (true) {
                    ConsumerRecords<String, String> poll = customer.poll(Duration.ofSeconds(2));
                    List<Map> updateList = dealData(poll);
                    EsClientUtils2.getBulkProcess().flush();
                    scheduler.schedule(() -> {
                        try {
                            updateSatusAll(updateList);
                        } finally {
                        }
                    }, 10, TimeUnit.SECONDS);
                    customer.commitSync();
                    customer.commitSync();
                }
            };
            this.thread_pool.execute(task);
        }
    }

    @Override
    public List<Map> dealData(ConsumerRecords<String, String> records) {
        List<Map> resutList = eventAction(records, "dataset");
        return resutList;

    }
}
