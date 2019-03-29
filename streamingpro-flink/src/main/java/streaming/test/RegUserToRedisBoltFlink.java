package streaming.test;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;

import java.util.HashMap;
import java.util.Map;

public class RegUserToRedisBoltFlink implements SinkFunction<Row> {

    private static final long serialVersionUID = 1L;
    private static Logger logger = LoggerFactory.getLogger(RegUserToRedisBoltFlink.class);
    private JedisCluster jedisHelper = new JedisCluster(new HostAndPort("1", 2));
    private Jedis jedis = null;
    private Pipeline pipeline = null;
    private int mills = 1000;
    private long preMill = 0;
    private Map<String, Long> hourDataMap = new HashMap<String, Long>();
    private Map<String, Long> dayDataMap = new HashMap<String, Long>();

    private Map<String, Map<String, Long>> topHourMap = new HashMap<>();
    private Map<String, Map<String, Long>> topDayMap = new HashMap<>();


    private String redisNodes;
    private String m_broker;

    public RegUserToRedisBoltFlink(String broker, String redisNodes, int sec) {
        this.redisNodes = redisNodes;
        this.m_broker = broker;
        this.mills = sec * 1000;
        logger.info("redis hosts:" + this.redisNodes);
        //jedisHelper = new JedisNodeMap(this.redisNodes);
//        jedisHelper = new JedisClusterHelper(this.redisNodes);
    }


    @Override
    public void invoke(Row value, Context context) throws Exception {
    }


}
