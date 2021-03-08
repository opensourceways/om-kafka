//package Utils;
//
//import Utils.HttpClientUtils;
//import org.apache.commons.lang.ArrayUtils;
//import org.apache.commons.lang.StringUtils;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
//import org.elasticsearch.action.bulk.BulkRequest;
//import org.elasticsearch.action.index.IndexRequest;
//import org.elasticsearch.action.search.ClearScrollRequest;
//import org.elasticsearch.action.search.SearchRequest;
//import org.elasticsearch.action.search.SearchResponse;
//import org.elasticsearch.action.search.SearchScrollRequest;
//import org.elasticsearch.client.RequestOptions;
//import org.elasticsearch.client.RestHighLevelClient;
//import org.elasticsearch.index.query.QueryBuilders;
//import org.elasticsearch.search.Scroll;
//import org.elasticsearch.search.SearchHit;
//import org.elasticsearch.search.builder.SearchSourceBuilder;
//import scala.collection.mutable.Buffer;
//
//import java.io.IOException;
//import java.io.InputStream;
//import java.util.*;
//
//import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;
//import static scala.collection.JavaConverters.asScalaBufferConverter;
//import static scala.collection.JavaConverters.mapAsJavaMapConverter;
//
//public class DataUtils {
//    /**
//     * 从ES按时间查询所有数据。若时间都为空，则查询全部数据
//     *
//     * @param index         index
//     * @param docType       type
//     * @param includes      包含哪些字段
//     * @param excludes      不包含哪些字段
//     * @param timeField     时间字段
//     * @param startDate     起始时间
//     * @param endDate       终止时间
//     * @param scrollTimeOut 游标超时时间
//     * @return hits
//     */
//    public List<SearchHit> searchAllFromEs(String index, String docType, String[] includes, String[] excludes,
//                                           String timeField, String startDate, String endDate, Long scrollTimeOut) {
//        RestHighLevelClient client = HttpClientUtils.restClient();
//
//        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//        searchSourceBuilder.fetchSource(includes, excludes);
//
//        String startTime = startDate + "T00:00:00+00:00";
//        String endTime = endDate + "T23:59:59+00:00";
//        if (StringUtils.isBlank(startDate) && StringUtils.isBlank(endDate)) {
//            searchSourceBuilder.query(QueryBuilders.matchAllQuery());
//        } else {
//            searchSourceBuilder.query(QueryBuilders.rangeQuery(timeField).gte(startTime).lte(endTime));
//        }
//
//        SearchRequest searchRequest = new SearchRequest(index);
//        searchRequest.types(docType);
//        searchRequest.source(searchSourceBuilder);
//
//        try {
//            //SCROLL_TIMEOUT是快照保存时间
//            return scrollSearchAll(client, scrollTimeOut, searchRequest);
//        } catch (IOException e) {
//            e.printStackTrace();
//            return null;
//        } finally {
//            try {
//                client.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//    }
//
//    /**
//     * 滚动查询数据，并释放资源
//     *
//     * @param restHighLevelClient client
//     * @param scrollTimeOut       游标超时时间
//     * @param searchRequest       查询请求
//     * @return hits
//     */
//    public List<SearchHit> scrollSearchAll(RestHighLevelClient restHighLevelClient, Long scrollTimeOut,
//                                           SearchRequest searchRequest) throws IOException {
//        Scroll scroll = new Scroll(timeValueMillis(scrollTimeOut));
//        searchRequest.scroll(scroll);
//        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
//        String scrollId = searchResponse.getScrollId();
//
//        SearchHit[] hits = searchResponse.getHits().getHits();
//        List<SearchHit> resultSearchHit = new ArrayList<>();
//        while (ArrayUtils.isNotEmpty(hits)) {
//            resultSearchHit.addAll(Arrays.asList(hits));
//
//            SearchScrollRequest searchScrollRequest = new SearchScrollRequest(scrollId);
//            searchScrollRequest.scroll(scroll);
//            SearchResponse searchScrollResponse = restHighLevelClient.scroll(searchScrollRequest, RequestOptions.DEFAULT);
//            scrollId = searchScrollResponse.getScrollId();
//            hits = searchScrollResponse.getHits().getHits();
//        }
//        //及时清除es快照，释放资源
//        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
//        clearScrollRequest.addScrollId(scrollId);
//        restHighLevelClient.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
//        return resultSearchHit;
//    }
//
//}
