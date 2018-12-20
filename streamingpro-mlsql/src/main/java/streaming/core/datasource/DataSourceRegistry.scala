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

package streaming.core.datasource

import com.google.common.reflect.ClassPath
import org.apache.spark.sql.SaveMode
import streaming.log.Logging

import scala.collection.JavaConverters._

/**
  * 2018-12-20 WilliamZhu(allwefantasy@gmail.com)
  */
object DataSourceRegistry extends Logging {
  private val registry = new java.util.concurrent.ConcurrentHashMap[String, MLSQLDataSource]()

  def register(name: String, obj: MLSQLDataSource) = {
    registry.put(name, obj)

  }

  def fetch(name: String): Option[MLSQLDataSource] = {
    if (registry.containsKey(name)) {
      Option(registry.get(name))
    } else None
  }

  private def registerFromPackage(name: String) = {
    ClassPath.from(getClass.getClassLoader).getTopLevelClasses(name).asScala.foreach { clzz =>
      val dataSource = Class.forName(clzz.getName).newInstance()
      if (dataSource.isInstanceOf[MLSQLRegistry]) {
        dataSource.asInstanceOf[MLSQLRegistry].register()
      } else {
        logWarning(
          s"""
             |${clzz.getName} does not implement MLSQLRegistry,
             |we cannot register it automatically.
         """.stripMargin)
      }
    }
  }

  registerFromPackage("streaming.core.datasource.impl")
  registerFromPackage("streaming.contri.datasource.impl")
}

trait MLSQLRegistry {
  def register(): Unit
}

case class DataSourceConfig(path: String, config: Map[String, String])

case class DataSinkConfig(path: String, config: Map[String, String], mode: SaveMode)
