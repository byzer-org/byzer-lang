package streaming.common.http;

import org.apache.http.*;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.LayeredConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContexts;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * HttpClient 工具类
 *
 * Created by zhuml on 2018/9/3.
 */
public class HttpClientUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpClientUtil.class);

    private static CloseableHttpClient httpClient = null;

    private final static Object syncLock = new Object();

    /**
     * 获取 HttpClient 对象
     */
    public static CloseableHttpClient getHttpClient() {
        if (httpClient == null) {
            synchronized (syncLock) {
                if (httpClient == null) {
                    httpClient = createHttpClient();
                }
            }
        }
        return httpClient;
    }

    /**
     * 创建 HttpClient 对象
     */
    public static CloseableHttpClient createHttpClient() {
        ConnectionSocketFactory plainConnectionSocketFactory = PlainConnectionSocketFactory.getSocketFactory();
        LayeredConnectionSocketFactory sslConnectionSocketFactory = SSLConnectionSocketFactory.getSocketFactory();

        try {
            sslConnectionSocketFactory = new SSLConnectionSocketFactory(SSLContexts.custom()
                                                                                .loadTrustMaterial(null, new TrustSelfSignedStrategy())
                                                                                .build());
        } catch (NoSuchAlgorithmException e) {
            LOGGER.error(e.getMessage(), e);
        } catch (KeyManagementException e) {
            LOGGER.error(e.getMessage(), e);
        } catch (KeyStoreException e) {
            LOGGER.error(e.getMessage(), e);
        }

        RegistryBuilder registryBuilder = RegistryBuilder.<ConnectionSocketFactory>create()
                .register("http", plainConnectionSocketFactory)
                .register("https", sslConnectionSocketFactory);
        Registry<ConnectionSocketFactory> registry = registryBuilder.build();

        PoolingHttpClientConnectionManager httpClientConnectionManager = new PoolingHttpClientConnectionManager(registry);

        // 最大连接数
        httpClientConnectionManager.setMaxTotal(HttpConstants.CLIENT_MAX_TOTAL);
        // 每个路由基础的连接
        httpClientConnectionManager.setDefaultMaxPerRoute(HttpConstants.CLIENT_DEFAULT_MAX_PER_ROUTE);

        // 请求重试处理
        HttpRequestRetryHandler httpRequestRetryHandler = new HttpRequestRetryHandler() {
            public boolean retryRequest(IOException exception,
                                        int executionCount, HttpContext context) {
                if (executionCount >= 5) { // 如果已经重试了5次，就放弃
                    return false;
                }
                if (exception instanceof NoHttpResponseException) { // 如果服务器丢掉了连接，那么就重试
                    return true;
                }
                if (exception instanceof SSLHandshakeException) { // 不要重试SSL握手异常
                    return false;
                }
                if (exception instanceof InterruptedIOException) { // 超时
                    return false;
                }
                if (exception instanceof UnknownHostException) { // 目标服务器不可达
                    return false;
                }
                if (exception instanceof ConnectTimeoutException) { // 连接被拒绝
                    return false;
                }
                if (exception instanceof SSLException) { // SSL握手异常
                    return false;
                }

                HttpClientContext clientContext = HttpClientContext.adapt(context);
                HttpRequest request = clientContext.getRequest();
                // 如果请求是幂等的，就再次尝试
                if (!(request instanceof HttpEntityEnclosingRequest)) {
                    return true;
                }
                return false;
            }
        };

        CloseableHttpClient httpClient = HttpClients.custom()
                .setConnectionManager(httpClientConnectionManager)
                .setRetryHandler(httpRequestRetryHandler)
                .build();

        return httpClient;
    }

    /**
     * 建立连接的基本属性配置
     */
    private static void setRequestConfig(HttpRequestBase httpRequestBase,
                                         HttpConstants.HttpClientConfig httpClientConfig) {
        // 设置 Heade 等

        // 配置请求的超时设置
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(httpClientConfig.getConnectTimeout())
                .setConnectionRequestTimeout(httpClientConfig.getConnectionRequestTimeout())
                .setSocketTimeout(httpClientConfig.getSocketTimeout())
                .build();
        httpRequestBase.setConfig(requestConfig);
    }

    /**
     * GET 请求
     *
     * @param url
     * @return
     */
    public static HttpResult doGet(String url) {
        return doGet(url, null, HttpConstants.HttpClientConfig.DEFAULT);
    }

    /**
     * GET 请求
     *
     * @param url
     * @param httpClientConfig
     * @return
     */
    public static HttpResult doGet(String url, HttpConstants.HttpClientConfig httpClientConfig) {
        return doGet(url, null, httpClientConfig);
    }

    /**
     * GET 请求
     *
     * @param url
     * @param params
     * @param httpClientConfig
     * @return
     */
    public static HttpResult doGet(String url, Map<String, Object> params, HttpConstants.HttpClientConfig httpClientConfig) {
        try {
            URIBuilder uriBuilder = new URIBuilder(url);
            if (params != null) {
                // 遍历map,拼接请求参数
                for (Map.Entry<String, Object> entry : params.entrySet()) {
                    uriBuilder.setParameter(entry.getKey(), entry.getValue().toString());
                }
            }
            url = uriBuilder.build().toString();

            HttpGet httpGet = new HttpGet(url);

            setRequestConfig(httpGet, httpClientConfig);

            CloseableHttpResponse response = getHttpClient().execute(httpGet);

            HttpEntity entity = response.getEntity();
            HttpResult result = new HttpResult(response.getStatusLine().getStatusCode(), EntityUtils.toString(entity, "UTF-8"));

            // 释放资源
            EntityUtils.consume(entity);
            httpGet.releaseConnection();

            return result;
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            return new HttpResult(0, "", e.getMessage());
        }
    }

    /**
     * POST 请求
     *
     * @param url
     * @return
     */
    public static HttpResult doPost(String url) {
        return doPost(url, null, HttpConstants.HttpClientConfig.DEFAULT);
    }

    /**
     * POST 请求
     *
     * @param url
     * @param httpClientConfig
     * @return
     */
    public static HttpResult doPost(String url, HttpConstants.HttpClientConfig httpClientConfig) {
        return doPost(url, null, httpClientConfig);
    }

    /**
     * POST 请求
     *
     * @param url
     * @param params
     * @param httpClientConfig
     * @return
     */
    public static HttpResult doPost(String url, Map<String, Object> params, HttpConstants.HttpClientConfig httpClientConfig) {
        HttpPost httpPost = new HttpPost(url);

        setRequestConfig(httpPost, httpClientConfig);

        setPostParams(httpPost, params);

        try {
            CloseableHttpResponse response = getHttpClient().execute(httpPost);

            HttpEntity entity = response.getEntity();
            HttpResult result = new HttpResult(response.getStatusLine().getStatusCode(), EntityUtils.toString(entity, "UTF-8"));

            // 释放资源
            EntityUtils.consume(response.getEntity());
            httpPost.releaseConnection();

            return result;
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            return new HttpResult(0, "", e.getMessage());
        }
    }

    /**
     * POST body 请求
     *
     * @param url
     * @param bodyJson
     * @param httpClientConfig
     * @return
     */
    public static HttpResult doPostBody(String url, String bodyJson, HttpConstants.HttpClientConfig httpClientConfig) {
        HttpPost httpPost = new HttpPost(url);

        setRequestConfig(httpPost, httpClientConfig);

        try {
            StringEntity entity = new StringEntity(bodyJson, "utf-8");
            httpPost.setEntity(entity);
            httpPost.setHeader("Content-type", "application/json");
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            return new HttpResult(0, "", e.getMessage());
        }

        try {
            CloseableHttpResponse response = getHttpClient().execute(httpPost);

            HttpEntity entity = response.getEntity();
            HttpResult result = new HttpResult(response.getStatusLine().getStatusCode(), EntityUtils.toString(entity, "UTF-8"));

            // 释放资源
            EntityUtils.consume(response.getEntity());
            httpPost.releaseConnection();

            return result;
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            return new HttpResult(0, "", e.getMessage());
        }
    }

    private static void setPostParams(HttpPost httpPost, Map<String, Object> params) {
        if (params != null && params.size() > 0) {
            // 进行遍历，封装from表单对象
            List<NameValuePair> list = new ArrayList<NameValuePair>();
            for (Map.Entry<String, Object> entry : params.entrySet()) {
                list.add(new BasicNameValuePair(entry.getKey(), entry.getValue().toString()));
            }
            try {
                UrlEncodedFormEntity urlEncodedFormEntity = new UrlEncodedFormEntity(list, "UTF-8");
                httpPost.setEntity(urlEncodedFormEntity);
            } catch (UnsupportedEncodingException e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

}
