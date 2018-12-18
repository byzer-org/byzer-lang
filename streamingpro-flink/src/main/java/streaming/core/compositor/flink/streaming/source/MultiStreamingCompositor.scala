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

package streaming.core.compositor.flink.streaming.source

import java.util
import java.util.Properties

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.table.api.TableEnvironment
import org.apache.log4j.Logger
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.core.compositor.flink.streaming.CompositorHelper
import streaming.core.strategy.platform.FlinkStreamingRuntime

import scala.collection.JavaConversions._


class MultiStreamingCompositor[T] extends Compositor[T] with CompositorHelper {

  private var _configParams: util.List[util.Map[Any, Any]] = _

  val logger = Logger.getLogger(classOf[MultiStreamingCompositor[T]].getName)

  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }

  private def getKafkaParams(params: util.Map[Any, Any]) = {
    params.filter {
      f =>
        if (f._1 == "topics") false else true
    }.toMap.asInstanceOf[Map[String, String]]
  }


  private def getZk(params: util.Map[Any, Any]) = {
    getKafkaParams(params).getOrElse("zk", getKafkaParams(params).getOrElse("zookeeper", getKafkaParams(params).getOrElse("zkQuorum", "127.0.0.1")))
  }

  private def getTopics(params: util.Map[Any, Any]) = {
    params.get("topics").asInstanceOf[String].split(",").toSet
  }

  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {
    val runtime = params.get("_runtime_").asInstanceOf[FlinkStreamingRuntime]
    val env = runtime.runtime
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    params.put("tableEnv",tableEnv)

    val kafkaListStream = _configParams.map { p =>

      p.getOrElse("format","-") match {
        case "kafka" =>
          val zk = getZk(p)
          val groupId = getKafkaParams(p).get("groupId").get
          val topics = getTopics(p).mkString(",")

          val properties = new Properties()
          properties.setProperty("zookeeper.connect", zk)
          properties.setProperty("group.id", groupId)

          implicit val typeInfo = TypeInformation.of(classOf[String])
          val kafkaStream = env.addSource(new FlinkKafkaConsumer08[String](topics, new SimpleStringSchema(), properties))

          tableEnv.registerDataStream[String](p("outputTable").toString, kafkaStream)
          kafkaStream
        case "socket" =>
          val socketStream = env.socketTextStream(
            p.getOrElse("host","localhost").toString,
            p.getOrElse("port","9000").toString.toInt,
            '\n')
          tableEnv.registerDataStream[String](p("outputTable").toString, socketStream)
          socketStream
        case _ =>
      }


    }

    List(kafkaListStream.asInstanceOf[T])
  }

}
