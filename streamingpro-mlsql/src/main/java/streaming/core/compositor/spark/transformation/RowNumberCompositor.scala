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

package streaming.core.compositor.spark.transformation

import java.util

import org.apache.log4j.Logger
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.core.CompositorHelper

/**
  * 8/2/16 WilliamZhu(allwefantasy@gmail.com)
  */
class RowNumberCompositor[T] extends Compositor[T] with CompositorHelper {
  private var _configParams: util.List[util.Map[Any, Any]] = _
  val logger = Logger.getLogger(classOf[SQLCompositor[T]].getName)

  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }

  def outputTableName = {
    config[String]("outputTableName", _configParams)
  }

  def inputTableName = {
    config[String]("inputTableName", _configParams)
  }

  def rankField = {
    config[String]("rankField", _configParams)
  }


  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {

    val context = sparkSession(params)
    import org.apache.spark.sql.types.{LongType, StructField, StructType}

    val _inputTableName = inputTableName.get
    val _outputTableName = outputTableName.get
    val _rankField = rankField.get

    val table = context.table(_inputTableName)
    val schema = table.schema
    val rdd = table.rdd
    val newSchema = new StructType(schema.fields ++ Array(StructField(_rankField, LongType)))

    val newRowsWithScore = rdd.zipWithIndex().map { f =>
      org.apache.spark.sql.Row.fromSeq(f._1.toSeq ++ Array(f._2))
    }

    context.createDataFrame(newRowsWithScore, newSchema).createOrReplaceTempView(_outputTableName)

    middleResult

  }

}
