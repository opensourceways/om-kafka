package test;

import om.QualityDashboard;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author xiazhonghai
 * @date 2021/1/13 9:26
 * @description:
 */
public class privider {
    static Properties conf = new Properties();
    static KafkaProducer<String, String> kafkaProducer;

    static {
        InputStream resourceAsStream = QualityDashboard.class.getClassLoader().getResourceAsStream("conf.properties");
        try {
            conf.load(resourceAsStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        conf.put("group.id", "QualityDashboard_obs_test");
        conf.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        conf.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProducer = new KafkaProducer<String, String>(conf);
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        RestHighLevelClient client = QualityDashboard.client;
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        SearchSourceBuilder query = searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        query.size(10000);
        SearchRequest searchRequest = new SearchRequest("openeuler_statewall_obs_job_history");
        searchRequest.source(query);
        Scroll scroll1 = new Scroll(TimeValue.timeValueMillis(10000));
        searchRequest.scroll(scroll1);
        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
        String scrollId = search.getScrollId();
        SearchHit[] hits = search.getHits().getHits();
        ArrayList<SearchHit> searchHits = new ArrayList<>();
        while(hits.length>0){
            searchHits.addAll(Arrays.asList(hits));
            SearchScrollRequest searchScrollRequest = new SearchScrollRequest(scrollId);
            searchScrollRequest.scroll(scroll1);
            SearchResponse scroll = client.scroll(searchScrollRequest, RequestOptions.DEFAULT);
            scrollId=scroll.getScrollId();
            hits=scroll.getHits().getHits();
            if(searchHits.size()>=10000){
                for (SearchHit searchHit : searchHits) {
                    ProducerRecord<String, String> record = new ProducerRecord<String, String>("obstest2", "key", searchHit.getSourceAsString());
                    Future<RecordMetadata> send = kafkaProducer.send(record);
                    RecordMetadata recordMetadata = send.get();
                    System.out.println(recordMetadata.offset());
                }
                searchHits.clear();
            }
        }
        for (SearchHit searchHit : searchHits) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("obstest2", "key", searchHit.getSourceAsString());
            Future<RecordMetadata> send = kafkaProducer.send(record);
            RecordMetadata recordMetadata = send.get();
            System.out.println(recordMetadata.offset());
        }
        searchHits.clear();
    }
}
