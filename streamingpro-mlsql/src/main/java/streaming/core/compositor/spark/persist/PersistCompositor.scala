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

package streaming.core.compositor.spark.persist

import java.util

import org.apache.log4j.Logger
import org.apache.spark.storage.StorageLevel
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.core.CompositorHelper

import scala.collection.JavaConversions._

class PersistCompositor[T] extends Compositor[T] with CompositorHelper {

  private var _configParams: util.List[util.Map[Any, Any]] = _
  val logger = Logger.getLogger(classOf[PersistCompositor[T]].getName)

  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }

  def tableName = {
    config[String]("tableName", _configParams)
  }

  def deserialized = {
    config[Boolean]("serialized", _configParams).getOrElse(false)
  }

  def useDisk = {
    config[Boolean]("disk", _configParams).getOrElse(true)
  }

  def useMemory = {
    config[Boolean]("memory", _configParams).getOrElse(true)
  }

  def replication = {
    config[Int]("replication", _configParams).getOrElse(1)
  }

  def storeLevel = {
    val ms = config[String]("storeLevel", _configParams)
    if (ms.isEmpty) {
      StorageLevel.apply(useDisk, useMemory, deserialized, replication)
    } else {
      StorageLevel.fromString(ms.get.toUpperCase)
    }
  }

  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {
    require(tableName.isDefined, "please set tableName  by variable `tableName` in config file")
    sparkSession(params).table(tableName.get).persist(storeLevel)

    List()
  }
}
