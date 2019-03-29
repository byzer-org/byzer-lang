package streaming.test

import java.util

import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.types.Row
import org.apache.flink.util.InstantiationUtil
import redis.clients.jedis.{HostAndPort, JedisCluster}
import streaming.core.compositor.flink.streaming.output.RedisSinkFunction

object MyTest2 {
  def main(args: Array[String]): Unit = {
//    InstantiationUtil.serializeObject(new RegUserToRedisBoltFlink("", "", 1))
    InstantiationUtil.serializeObject(new RedisSinkFunction(null, 1, null, null))
//    InstantiationUtil.serializeObject(new SinkFunction[Row] {
//      private  var  jedisCluster :JedisCluster = null
//
//      override def invoke(row: Row, context: SinkFunction.Context[_]): Unit = {
//        if (jedisCluster == null) {
//          this.synchronized {
//            if (jedisCluster == null) {
//              jedisCluster = new JedisCluster(new HostAndPort("12", 1), 2)
//            }
//          }
//        }
//      }
//
//
//        })

  }
}
