package Utils;

import om.QualityDashboard;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Map;

/**
 * @author xiazhonghai
 * @date 2021/3/4 9:03
 * @description:
 */
public class EsClientUtils2 {
    private static Logger logger = LogManager.getLogger(QualityDashboard.class);
    private static BulkProcessor build = null;
    private static RestHighLevelClient client = null;
    private static CredentialsProvider credentialsProvider = new BasicCredentialsProvider();

    static {
        try {
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(PropertiesUtils.readProperties().get("es.user").toString(), PropertiesUtils.readProperties().get("es.password").toString()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static BulkProcessor.Listener getBulkListener() {

        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                logger.info(String.format("before push  %s request", request.numberOfActions()));
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {

            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {

            }
        };
        return listener;
    }

    public static synchronized RestHighLevelClient getClient() throws IOException {
        if (client == null) {
            client = new RestHighLevelClient(
                    RestClient.builder(
                            new HttpHost(PropertiesUtils.readProperties().get("es.host").toString(), Integer.parseInt(PropertiesUtils.readProperties().get("es.port").toString()), PropertiesUtils.readProperties().get("es.scheme").toString())
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
        }
        return client;
    }

    public static synchronized BulkProcessor getBulkProcess() {
        if (build == null) {

            build = BulkProcessor.builder((request, bulkListener) -> {
                try {
                    getClient().bulkAsync(request, RequestOptions.DEFAULT, bulkListener);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }, getBulkListener())
                    // 1000条数据请求执行一次bulk
                    .setBulkActions(1000)
                    // 5mb的数据刷新一次bfulk
                    .setBulkSize(new ByteSizeValue(5L, ByteSizeUnit.MB))
                    // 并发请求数量, 0不并发, 1并发允许执行
                    .setConcurrentRequests(1)
                    // 固定1s必须刷新一次
                    .setFlushInterval(TimeValue.timeValueSeconds(1L))
                    // 重试5次，间隔1s
                    .setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(1L), 5))
                    .build();
        }
        return build;
    }
    public static void insertOrUpdate(String index, String id, Map data) throws IOException {
        UpdateRequest updateRequest = new UpdateRequest(index, id);
        updateRequest.doc(data);
        updateRequest.upsert(data);
        try {
            UpdateResponse update = getClient().update(updateRequest, RequestOptions.DEFAULT);
        }catch (Exception e){
            //todo 连接超时，失败是重试策略
            e.printStackTrace();
        }
    }
    public static SSLContext skipSsl() throws NoSuchAlgorithmException, KeyManagementException {
        SSLContext sc = SSLContext.getInstance("SSL");

        // 实现一个X509TrustManager接口，用于绕过验证，不用修改里面的方法
        X509TrustManager trustManager = new X509TrustManager() {
            @Override
            public void checkClientTrusted(
                    java.security.cert.X509Certificate[] paramArrayOfX509Certificate,
                    String paramString) throws CertificateException {
            }

            @Override
            public void checkServerTrusted(
                    java.security.cert.X509Certificate[] paramArrayOfX509Certificate,
                    String paramString) throws CertificateException {
            }

            @Override
            public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                return null;
            }
        };

        sc.init(null, new TrustManager[]{trustManager}, null);
        return sc;
    }
}
