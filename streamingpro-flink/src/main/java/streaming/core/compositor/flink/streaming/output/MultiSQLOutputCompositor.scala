/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package streaming.core.compositor.flink.streaming.output

import java.util

import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.sinks.{ConsoleTableSink, CsvTableSink}
import org.apache.flink.types.Row
import org.apache.log4j.Logger
import redis.clients.jedis.{HostAndPort, JedisCluster}
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.core.compositor.flink.streaming.CompositorHelper
import streaming.core.strategy.ParamsValidator
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import streaming.utils.CollectionUtils

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
  * Created by allwefantasy on 20/3/2017.
  */
class MultiSQLOutputCompositor[T] extends Compositor[T] with CompositorHelper with ParamsValidator {

  private var _configParams: util.List[util.Map[Any, Any]] = _
  val logger = Logger.getLogger(classOf[MultiSQLOutputCompositor[T]].getName)


  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }




  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {
    val tableEnv = params.get("tableEnv").asInstanceOf[TableEnvironment]

    _configParams.foreach { config =>

      val name = config.getOrElse("name", "").toString
      val _cfg = config.map(f => (f._1.toString, f._2.toString)).map { f =>
        (f._1, params.getOrElse(s"streaming.sql.out.${name}.${f._1}", f._2).toString)
      }.toMap

      val tableName = _cfg("inputTableName")
      val options = _cfg - "path" - "mode" - "format"
      val _resource = _cfg.getOrElse("path","-")
      val mode = _cfg.getOrElse("mode", "ErrorIfExists")
      val format = _cfg("format")
      val showNum = _cfg.getOrElse("showNum", "100").toInt


      val ste = tableEnv.asInstanceOf[StreamTableEnvironment]

      format match {
        case "csv" | "com.databricks.spark.csv" =>
          val csvTableSink = new CsvTableSink(_resource)
          ste.scan(tableName).writeToSink(csvTableSink)
        case "console" | "print" =>
          ste.scan(tableName).writeToSink(new ConsoleTableSink(showNum))
        case "redis" =>
          writeToRedis(ste, tableName, _cfg)
        case _ =>
      }
    }

    new util.ArrayList[T]()
  }

  override def valid(params: util.Map[Any, Any]): (Boolean, String) = {
    (true, "")
  }



  def writeToRedis(ste: StreamTableEnvironment, tableName: String, _cfg: Map[String, String]) = {
    val redisHosts :java.util.Set[HostAndPort] = _cfg.get("hosts").get.split(",").map(str => {
      val host = str.split(":")(0)
      val port = str.split(":")(1).toInt
      new HostAndPort(host, port)
    }).toSet.asJava
      val timeout = _cfg.getOrElse("timeout", 10000).toString.toInt

         val columnNameToIndex = CollectionUtils.list2Map(ste.scan(tableName).getSchema.getFieldNames)
         ste.toAppendStream[Row](ste.scan(tableName)).addSink(new RedisSinkFunction(redisHosts, timeout, _cfg, columnNameToIndex))

  }
}

class RedisSinkFunction(redisHosts: java.util.Set[HostAndPort], timeout:Int, _cfg:Map[String, String], columnNameToIndex:Map[String, Int] ) extends  SinkFunction[Row] {
  private  var  jedisCluster :JedisCluster = _

  override def invoke(row: Row, context: SinkFunction.Context[_]): Unit = {
        if(jedisCluster == null) {
          this.synchronized {
              if (jedisCluster == null) {
                  jedisCluster = new JedisCluster(redisHosts, timeout)
              }
            }
         }
      _cfg("operators").asInstanceOf[util.List[util.Map[Any, Any]]].foreach(operator => {
      operator("type") match {
      case "incrBy" =>
      val key = operator("key").toString
      val incrementValue = operator("incrementValue").toString
      val expire = operator.getOrElse("expire", "").toString
      val keyAttr = row.getField(columnNameToIndex(key)).toString
      val increValue = row.getField(columnNameToIndex(incrementValue)).toString.toLong
      jedisCluster.incrBy(keyAttr, increValue)
      if(expire != "") {
          jedisCluster.expire(keyAttr, expire.toInt)
      }
      case "zincrBy" =>
      val key = operator("key").toString
      val score = _cfg("score")
      val value = _cfg("value")
      val expire = _cfg.getOrElse("expire", "").toString
      val keyAttr = row.getField(columnNameToIndex(key)).toString
      val scoreAttr = row.getField(columnNameToIndex(score)).toString.toLong
      val valueAttr = row.getField(columnNameToIndex(value)).toString
      jedisCluster.zincrby(keyAttr, scoreAttr, valueAttr)
      if(expire != "") {
          jedisCluster.expire(keyAttr, expire.toInt)
      }
}

})
}

}


