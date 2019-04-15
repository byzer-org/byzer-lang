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
import org.apache.spark.sql.{DataFrame, SQLContext}
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.core.CompositorHelper

/**
  * 8/2/16 WilliamZhu(allwefantasy@gmail.com)
  */
abstract class BaseDFCompositor[T] extends Compositor[T] with CompositorHelper {
  protected var _configParams: util.List[util.Map[Any, Any]] = _
  val logger = Logger.getLogger(classOf[SQLCompositor[T]].getName)

  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }


  val TABLE = "_table_"
  val FUNC = "_func_"

  def outputTableName = {
    config[String]("outputTableName", _configParams)
  }

  def inputTableName = {
    config[String]("inputTableName", _configParams)
  }


  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {

    if (!params.containsKey(TABLE) && !params.containsKey(FUNC)) {
      val df = sparkSession(params).table(inputTableName.get)
      val newDf = createDF(params, df)
      newDf.createOrReplaceTempView(outputTableName.get)
      return middleResult
    }

    if (params.containsKey(TABLE)) {
      //parent compositor is  tableCompositor

      val func = params.get(TABLE).asInstanceOf[(Any) => SQLContext]
      params.put(FUNC, (rddOrDF: Any) => {
        val sqlContext = func(rddOrDF)
        val newDF = createDF(params, sqlContext.table(inputTableName.get))
        outputTableName match {
          case Some(tableName) =>
            newDF.createOrReplaceTempView(tableName)
          case None =>
        }
        newDF
      })

    } else {
      // if not ,parent is SQLCompositor
      val func = params.get(FUNC).asInstanceOf[(DataFrame) => DataFrame]
      params.put(FUNC, (df: DataFrame) => {
        val newDF = createDF(params, func(df).sqlContext.table(inputTableName.get))
        outputTableName match {
          case Some(tableName) =>
            newDF.createOrReplaceTempView(tableName)
          case None =>
        }
        newDF
      })
    }
    params.remove(TABLE)

    middleResult

  }

  def createDF(params: util.Map[Any, Any], df: DataFrame): DataFrame

}
