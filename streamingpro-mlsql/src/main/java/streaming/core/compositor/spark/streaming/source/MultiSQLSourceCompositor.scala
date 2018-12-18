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

package streaming.core.compositor.spark.streaming.source

import java.util

import kafka.serializer.StringDecoder
import org.apache.log4j.Logger
import org.apache.spark.streaming.kafka.KafkaUtils
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.core.CompositorHelper
import streaming.core.strategy.platform.{PlatformManager, SparkStreamingRuntime}

import scala.collection.JavaConversions._

/**
  * 11/21/16 WilliamZhu(allwefantasy@gmail.com)
  */
class MultiSQLSourceCompositor[T] extends Compositor[T] with CompositorHelper {
  private var _configParams: util.List[util.Map[Any, Any]] = _

  val logger = Logger.getLogger(classOf[MultiSQLSourceCompositor[T]].getName)

  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }

  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {
    val spark = sparkSession(params)
    val runtime = PlatformManager.getRuntime.asInstanceOf[SparkStreamingRuntime]

    def zkEnable(_cfg: Map[String, String]) = {
      _cfg.containsKey("zk") ||
        _cfg.containsKey("zookeeper") ||
        _cfg.containsKey("zkQuorum")
    }

    def getZk(_cfg: Map[String, String]) = {
      _cfg.getOrElse("zk", _cfg.getOrElse("zookeeper", _cfg.getOrElse("zkQuorum", "127.0.0.1")))
    }

    def getgroupId(_cfg: Map[String, String]) = {
      _cfg.getOrElse("groupId", _cfg.getOrElse("groupid", _cfg.getOrElse("group", "127.0.0.1")))
    }

    def getTopics(_cfg: Map[String, String]) = {
      _cfg.getOrElse("topics", "").split(",").toSet
    }

    val dstreams = _configParams.map { sourceConfig =>

      val name = sourceConfig.getOrElse("name", "").toString
      val _cfg = sourceConfig.map(f => (f._1.toString, f._2.toString)).map { f =>
        (f._1, params.getOrElse(s"streaming.sql.source.${name}.${f._1}", f._2).toString)
      }.toMap

      val sourcePath = _cfg("path")
      val tableName = _cfg.getOrElse("outputTable", _cfg.getOrElse("outputTableName", ""))

      _cfg("format") match {
        case "kafka" =>

          (KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
            runtime.streamingContext,
            _cfg - "format",
            getTopics(_cfg)), tableName)

        //        case "kafka" if !_cfg.containsKey("direct") =>
        //          (KafkaUtils.createStream(runtime.streamingContext, getZk(_cfg), getgroupId(_cfg), getTopics(_cfg).map(f => (f, 1)).toMap)
        //            , tableName)

        case "socket" =>
          (runtime.streamingContext.socketTextStream(_cfg.getOrElse("host", "localhost"), _cfg.getOrElse("port", "9999").toInt).map(f => (null, f)),
            tableName)

        case _ =>
          val df = spark.read.format(_cfg("format")).options(
            (_cfg - "format" - "path" - "outputTable" - "data").map(f => (f._1.toString, f._2.toString))).load(sourcePath)
          df.createOrReplaceTempView(tableName)
          null
      }

    }
    require(dstreams.filter(f => f != null).size == 1, "for now ,only support one streaming source with multi batch sources")
    List(dstreams.filter(f => f != null).asInstanceOf[T])
  }
}
