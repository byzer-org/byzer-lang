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
import org.apache.spark.sql.{DataFrame, SaveMode}
import streaming.log.Logging

import scala.collection.JavaConverters._

/**
  * 2018-12-20 WilliamZhu(allwefantasy@gmail.com)
  */
object DataSourceRegistry extends Logging {
  private val registry = new java.util.concurrent.ConcurrentHashMap[MLSQLDataSourceKey, MLSQLDataSource]()

  def register(name: MLSQLDataSourceKey, obj: MLSQLDataSource) = {
    registry.put(name, obj)

  }

  def fetch(name: String, option: Map[String, String] = Map()): Option[MLSQLDataSource] = {
    val sourceType = if (option.contains("directQuery")) {
      MLSQLDirectDataSourceType
    } else {
      MLSQLSparkDataSourceType
    }
    val key = MLSQLDataSourceKey(name, sourceType)
    if (registry.containsKey(key)) {
      Option(registry.get(key))
    } else None
  }

  def findAllNames(name: String): Seq[String] = {
    val item = registry.asScala.filter(f => f._1.name == name).head
    Seq(item._2.shortFormat, item._2.fullFormat)
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

case class DataSourceConfig(path: String, config: Map[String, String], df: Option[DataFrame] = None)

case class DataSinkConfig(path: String, config: Map[String, String], mode: SaveMode, df: Option[DataFrame] = None)

case class MLSQLDataSourceKey(name: String, sourceType: MLSQLDataSourceType)

sealed abstract class MLSQLDataSourceType

case object MLSQLSparkDataSourceType extends MLSQLDataSourceType

case object MLSQLDirectDataSourceType extends MLSQLDataSourceType

case class DataAuthConfig(path: String, config: Map[String, String])