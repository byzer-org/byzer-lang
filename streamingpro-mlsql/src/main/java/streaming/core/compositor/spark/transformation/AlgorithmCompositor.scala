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
import org.apache.spark.ml.BaseAlgorithmTransformer
import org.apache.spark.sql.DataFrame
import serviceframework.dispatcher.{Processor, Strategy}

import scala.collection.JavaConversions._

/**
  * 7/27/16 WilliamZhu(allwefantasy@gmail.com)
  */
class AlgorithmCompositor[T] extends BaseAlgorithmCompositor[T] {


  val logger = Logger.getLogger(classOf[SQLCompositor[T]].getName)

  override def mapping = Map(
    "als" -> "org.apache.spark.ml.algs.ALSTransformer",
    "lr" -> "org.apache.spark.ml.algs.LinearRegressionTransformer",
    "lr2" -> "org.apache.spark.ml.algs.LogicRegressionTransformer"
  )


  override def result(processors: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {

    if (!params.containsKey(FUNC)) {
      val _inputTableName = inputTableName.get

      val input = sparkSession(params).table(_inputTableName)
      val output = algorithm(path).asInstanceOf[BaseAlgorithmTransformer].transform(input)

      outputTableName match {
        case Some(name) if name != null && name != "-" && !name.isEmpty =>
          output.createOrReplaceTempView(name)
        case None =>
      }

      return if (middleResult == null) List() else middleResult

    } else {
      val func = params.get(FUNC).asInstanceOf[(DataFrame) => DataFrame]
      params.put(FUNC, (df: DataFrame) => {
        val newDF = algorithm(path).asInstanceOf[BaseAlgorithmTransformer].transform(func(df))
        outputTableName match {
          case Some(tableName) =>
            newDF.createOrReplaceTempView(tableName)
          case None =>
        }
        newDF
      })

      params.remove(TABLE)
      return middleResult
    }

  }
}
