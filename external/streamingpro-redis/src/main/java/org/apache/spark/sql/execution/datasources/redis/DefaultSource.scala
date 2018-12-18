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

package org.apache.spark.sql.execution.datasources.redis

import com.redislabs.provider.redis.{RedisConfig, RedisEndpoint, RedisNode}
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import redis.clients.jedis.{Jedis, JedisSentinelPool, Protocol}
import redis.clients.util.JedisClusterCRC16
import scala.collection.JavaConversions._

/**
  * Created by allwefantasy on 19/11/2017.
  */
class DefaultSource extends RelationProvider with CreatableRelationProvider with DataSourceRegister {


  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = ???

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    val relation =
      if (parameters.contains("sentinelRedisEnable")) {
        InsertSentinelRedisRelation(data, parameters, mode)(sqlContext)
      } else {
        InsertRedisRelation(data, parameters, mode)(sqlContext)
      }

    relation.insert(data, mode == SaveMode.Overwrite)
    relation
  }

  override def shortName(): String = "redis"
}

case class InsertSentinelRedisRelation(
                                        dataFrame: DataFrame,
                                        parameters: Map[String, String], mode: SaveMode
                                      )(@transient val sqlContext: SQLContext)
  extends BaseRelation with InsertableRelation with Logging {

  val tableName: String = parameters.getOrElse("outputTableName", "PANIC")

  val host = parameters.getOrElse("host", Protocol.DEFAULT_HOST)
  val master_name = parameters.getOrElse("master_name", null)
  val port = parameters.getOrElse("port", Protocol.DEFAULT_PORT.toString).toInt
  val auth = parameters.getOrElse("auth", null)
  val dbNum = parameters.getOrElse("dbNum", Protocol.DEFAULT_DATABASE.toString).toInt
  val timeout = parameters.getOrElse("timeout", Protocol.DEFAULT_TIMEOUT.toString).toInt

  val sentinels = new java.util.HashSet[String]()
  sentinels.add(host + ":" + port)

  val pool = new JedisSentinelPool(master_name, sentinels, new GenericObjectPoolConfig(), timeout, auth, dbNum)


  override def schema: StructType = dataFrame.schema


  def redis_write(block: Jedis => Unit) = {
    val conn = pool.getResource
    val pipeline = conn.pipelined
    try {
      block(conn)
    } catch {
      case e: Exception =>
        log.info("redis write error", e)
        throw e
    } finally {
      pipeline.sync
      conn.close
    }

  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    parameters.get("insertType") match {
      case Some("listInsert") =>
        redis_write { pipeline =>
          val list_data = data.collect().map(f => f.getString(0))
          if (overwrite) {
            pipeline.del(tableName, tableName)
          }
          list_data.foreach(f => pipeline.lpush(tableName, f))
          if (parameters.contains("expire")) {
            pipeline.expire(tableName, time_parse(parameters.get("expire").get))
          }
        }

      case Some("listInsertAsString") =>
        redis_write { pipeline =>
          val list_data = data.collect().map(f => f.getString(0)).mkString(parameters.getOrElse("join", ",").toString())
          if (overwrite) {
            pipeline.del(tableName, tableName)
          }
          pipeline.set(tableName, list_data)
          if (parameters.contains("expire")) {
            pipeline.expire(tableName, time_parse(parameters.get("expire").get))
          }
        }
      case Some("KVList") =>
        redis_write { pipeline =>
          val list_data = data.collect().map(f => (f.getString(0), f.getList[String](1))).map { f =>
            pipeline.set(f._1, f._2.mkString(parameters.getOrElse("join", ",").toString()))
            if (parameters.contains("expire")) {
              pipeline.expire(f._1, time_parse(parameters.get("expire").get))
            }
          }
        }
      case None =>
    }
  }

  def time_parse(time: String) = {
    if (time.endsWith("h")) {
      time.split("h").head.toInt * 60 * 60
    } else if (time.endsWith("d")) {
      time.split("d").head.toInt * 60 * 60 * 24
    }
    else if (time.endsWith("m")) {
      time.split("m").head.toInt * 60
    }
    else if (time.endsWith("s")) {
      time.split("s").head.toInt
    }
    else {
      time.toInt
    }

  }
}

case class InsertRedisRelation(
                                dataFrame: DataFrame,
                                parameters: Map[String, String], mode: SaveMode
                              )(@transient val sqlContext: SQLContext)
  extends BaseRelation with InsertableRelation with Logging {

  val tableName: String = parameters.getOrElse("outputTableName", "PANIC")

  val redisConfig: RedisConfig = {
    new RedisConfig({
      if ((parameters.keySet & Set("host", "port", "auth", "dbNum", "timeout")).size == 0) {
        new RedisEndpoint(sqlContext.sparkContext.getConf)
      } else {
        val host = parameters.getOrElse("host", Protocol.DEFAULT_HOST)
        val port = parameters.getOrElse("port", Protocol.DEFAULT_PORT.toString).toInt
        val auth = parameters.getOrElse("auth", null)
        val dbNum = parameters.getOrElse("dbNum", Protocol.DEFAULT_DATABASE.toString).toInt
        val timeout = parameters.getOrElse("timeout", Protocol.DEFAULT_TIMEOUT.toString).toInt
        new RedisEndpoint(host, port, auth, dbNum, timeout)
      }
    }
    )
  }

  override def schema: StructType = dataFrame.schema

  def getNode(key: String): RedisNode = {
    val slot = JedisClusterCRC16.getSlot(key)
    /* Master only */
    redisConfig.hosts.filter(node => {
      node.startSlot <= slot && node.endSlot >= slot
    }).filter(_.idx == 0)(0)
  }

  def getNode(): RedisNode = {
    redisConfig.hosts.head
  }

  def redis_write(block: Jedis => Unit) = {
    val conn = getNode().connect
    val pipeline = conn.pipelined
    try {
      block(conn)
    } catch {
      case e: Exception =>
        log.info("redis write error", e)
        throw e
    } finally {
      pipeline.sync
      conn.close
    }

  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    parameters.get("insertType") match {
      case Some("listInsert") =>
        redis_write { pipeline =>
          val list_data = data.collect().map(f => f.getString(0))
          if (overwrite) {
            pipeline.del(tableName, tableName)
          }
          list_data.foreach(f => pipeline.lpush(tableName, f))
          if (parameters.contains("expire")) {
            pipeline.expire(tableName, time_parse(parameters.get("expire").get))
          }
        }

      case Some("listInsertAsString") =>
        redis_write { pipeline =>
          val list_data = data.collect().map(f => f.getString(0)).mkString(parameters.getOrElse("join", ",").toString())
          if (overwrite) {
            pipeline.del(tableName, tableName)
          }
          pipeline.set(tableName, list_data)
          if (parameters.contains("expire")) {
            pipeline.expire(tableName, time_parse(parameters.get("expire").get))
          }
        }
      case Some("KVList") =>
        redis_write { pipeline =>
          val list_data = data.collect().map(f => (f.getString(0), f.getList[String](1))).map { f =>
            pipeline.set(f._1, f._2.mkString(parameters.getOrElse("join", ",").toString()))
            if (parameters.contains("expire")) {
              pipeline.expire(f._1, time_parse(parameters.get("expire").get))
            }
          }
        }
      case None =>
    }
  }

  def time_parse(time: String) = {
    if (time.endsWith("h")) {
      time.split("h").head.toInt * 60 * 60
    } else if (time.endsWith("d")) {
      time.split("d").head.toInt * 60 * 60 * 24
    }
    else if (time.endsWith("m")) {
      time.split("m").head.toInt * 60
    }
    else if (time.endsWith("s")) {
      time.split("s").head.toInt
    }
    else {
      time.toInt
    }

  }
}
