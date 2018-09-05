package streaming.common.http;

/**
 * Created by zhuml on 2018/9/3.
 */
public class HttpConstants {

    public static final int CLIENT_MAX_TOTAL = 100;

    public static final int CLIENT_DEFAULT_MAX_PER_ROUTE = 20;

    public enum HttpClientConfig {

        DEFAULT(60000, 30000, 60000),
        SKONE(1200000, 30000, 1200000);

        HttpClientConfig(int connectTimeout,
                         int connectionRequestTimeout,
                         int socketTimeout) {
            this.connectTimeout = connectTimeout;
            this.connectionRequestTimeout = connectionRequestTimeout;
            this.socketTimeout = socketTimeout;
        }

        private int connectTimeout;
        private int connectionRequestTimeout;
        private int socketTimeout;

        public int getConnectTimeout() {
            return connectTimeout;
        }

        public int getConnectionRequestTimeout() {
            return connectionRequestTimeout;
        }

        public int getSocketTimeout() {
            return socketTimeout;
        }
    }

}
