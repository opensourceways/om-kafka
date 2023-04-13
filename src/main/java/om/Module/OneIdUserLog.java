/* This project is licensed under the Mulan PSL v2.
 You can use this software according to the terms and conditions of the Mulan PSL v2.
 You may obtain a copy of Mulan PSL v2 at:
     http://license.coscl.org.cn/MulanPSL2
 THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, EITHER EXPRESS OR
 IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR
 PURPOSE.
 See the Mulan PSL v2 for more details.
 Create: 2022
*/

package om.Module;

import CommonClass.CommonInterface;
import CommonClass.Parent;
import com.obs.services.ObsClient;
import com.obs.services.model.AppendObjectRequest;
import com.obs.services.model.ObjectMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

public class OneIdUserLog extends Parent implements CommonInterface {
    private static final Logger logger = LogManager.getLogger(QualitySoftWare.class);
    private static ObsClient obsClient = null;

    public OneIdUserLog() throws IOException {
        String ak = properties.get("obs.ak").toString();
        String sk = properties.get("obs.sk").toString();
        String endpoint = properties.get("obs.endpoint").toString();
        obsClient = new ObsClient(ak, sk, endpoint);
    }

    @Override
    public void run() {
        while (true) {
            for (KafkaConsumer<String, String> customer : this.KafkaConsumerList) {
                ConsumerRecords<String, String> poll = customer.poll(Duration.ofSeconds(2));
                dealData(poll);
                customer.commitSync();
            }
        }
    }


    @Override
    public List<Map> dealData(ConsumerRecords<String, String> records) {
        int count = 0;
        for (ConsumerRecord<String, String> record : records) {
            String value = record.value();
            try {
                ObsAppend(value);
                count += 1;
            } catch (Exception e) {
                e.printStackTrace();
                logger.error(e.getMessage() + ":" + value, e);
            }
        }
        if (count != 0) {
            System.out.println("*** obs append count: " + count);
        }
        return null;
    }

    private void ObsAppend(String context) {
        String bucketName = properties.get("obs.bucket.name").toString();
        String objectName = properties.get("obs.project.name").toString();
        String dateToday = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        objectName = objectName + "-" + dateToday + ".log";

        AppendObjectRequest request = new AppendObjectRequest();
        request.setBucketName(bucketName);
        request.setObjectKey(objectName);

        if (!obsClient.doesObjectExist(bucketName, objectName)) {
            request.setPosition(0);  // 首次写入
            request.setInput(new ByteArrayInputStream(context.getBytes()));
        } else {
            ObjectMetadata metadata = obsClient.getObjectMetadata(bucketName, objectName);
            request.setPosition(metadata.getNextPosition());  // 追加写入
            request.setInput(new ByteArrayInputStream(("\n" + context).getBytes()));
        }
        obsClient.appendObject(request);
    }
}
