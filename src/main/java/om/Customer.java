package om;

import Utils.EsClientUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;

import javax.net.ssl.SSLContext;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static Utils.EsClientUtils.skipSsl;

/**
 * @author xiazhonghai
 * @date 2021/1/21 9:00
 * @description:
 */
public class Customer extends Thread {
    private List<TopicPartition> topicPartitions=new ArrayList<>();
    private Properties conf;
    private KafkaConsumer kafkaConsumer;
    private CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    private  Logger logger = LogManager.getLogger(QualityDashboard.class);
    private  Logger loggeroffset ;
    private RestHighLevelClient client ;
    private String esindex ;

    @Override
    public void run() {
        while (true) {
            ConsumerRecords data = kafkaConsumer.poll(10);
            try {
                if(data.count()>0){
                    dealSoftWaredata(data);
                }
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }

        }
    }

    public Customer(TopicPartition topicPartition, Properties conf, String name) {
        super(name);
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(conf.get("es.user").toString(), conf.get("es.password").toString()));
        this.topicPartitions.add(topicPartition);
        this.conf = conf;
        kafkaConsumer = new KafkaConsumer(this.conf);
    }
    public Customer(TopicPartition topicPartition, Properties conf, String name,int offset,Logger logger) {
        super(name);
        this.loggeroffset=logger;
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(conf.get("es.user").toString(), conf.get("es.password").toString()));
        this.topicPartitions.add(topicPartition);
        this.conf = conf;
        kafkaConsumer = new KafkaConsumer(this.conf);
        kafkaConsumer.assign(this.topicPartitions);
        kafkaConsumer.seek(topicPartition,offset);
        client= new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(conf.get("es.host").toString(), Integer.parseInt(conf.get("es.port").toString()), conf.get("es.scheme").toString())
                ).setHttpClientConfigCallback(httpAsyncClientBuilder -> {
                    httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    SSLContext sc = null;
                    try {
                        sc = skipSsl();
                    } catch (NoSuchAlgorithmException | KeyManagementException e) {
                        e.printStackTrace();
                    }
                    return httpAsyncClientBuilder.setSSLContext(sc);
                }));
        esindex= conf.get("es.index").toString();
    }

    public void dealSoftWaredata(ConsumerRecords<String, String> datas) throws JsonProcessingException {
        BulkProcessor bulkProcess = EsClientUtils.getBulkProcess(client);
        ObjectMapper mapper = new ObjectMapper();
        for (ConsumerRecord<String, String> data : datas) {
            try {
                Map datamap = mapper.readValue(data.value(), Map.class);
                //更新以前旧数据
                if("succeeded".equals(datamap.get("code").toString())){
                    String packagename = datamap.get("package").toString();
                    String hostarch = datamap.get("hostarch").toString();
                    String project = datamap.get("project").toString();
                    UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest(esindex);
                    updateByQueryRequest.setQuery(new TermQueryBuilder("package", packagename));
                    updateByQueryRequest.setQuery(QueryBuilders.boolQuery().must(new TermQueryBuilder("package.keyword", packagename)).must(new TermQueryBuilder("hostarch.keyword", hostarch)).must(new TermQueryBuilder("project.keyword", project)).must(new TermQueryBuilder("is_latest_record_by_packagename", 1)));
                    updateByQueryRequest.setScript(new Script("ctx._source['is_latest_record_by_packagename']=0"));
                    BulkByScrollResponse updateResponse = client.updateByQuery(updateByQueryRequest, RequestOptions.DEFAULT);
                }
                //设置此条数据为这个包下的最新数据
                datamap.put("is_latest_record_by_packagename", 1);
                String workerid = datamap.get("workerid").toString();
                if(workerid !=null){
                    String newworkerid=workerid.split(":")[0];
                    datamap.put("abbrworkerid",newworkerid);
                }
                bulkProcess.add(new IndexRequest(esindex).id(datamap.get("created_at").toString() + datamap.get("package").toString() + datamap.get("bcnt")).source(datamap));
                loggeroffset.info("partition-" + data.partition() + "-offset-" + data.offset());
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
        bulkProcess.flush();
        bulkProcess.close();
    }
}
