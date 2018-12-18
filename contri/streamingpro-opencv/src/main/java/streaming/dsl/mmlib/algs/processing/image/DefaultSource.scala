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

package streaming.dsl.mmlib.algs.processing.image

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SaveMode, _}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

/**
  * Created by allwefantasy on 29/5/2018.
  */
class DefaultSource extends RelationProvider with CreatableRelationProvider with DataSourceRegister {
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    ImageRelation(parameters, None)(sqlContext)
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    null
  }

  override def shortName(): String = "Image"
}

case class ImageRelation(
                          parameters: Map[String, String],
                          userSpecifiedschema: Option[StructType]
                        )(@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan with Logging {
  override def schema: StructType = ImageSchema.imageDFSchema

  override def buildScan(): RDD[Row] = {
    val path = parameters("path")
    val recursive = parameters.getOrElse("recursive", "false").toBoolean
    val dropImageFailures = parameters.getOrElse("dropImageFailures", "false").toBoolean
    val sampleRatio = parameters.getOrElse("sampleRatio", "1.0").toDouble
    val numPartitions = parameters.getOrElse("numPartitions", "8").toInt
    val repartitionNum = parameters.getOrElse("repartitionNum", "0").toInt
    val filterByteSize = parameters.getOrElse("filterByteSize", "0").toInt
    val enableDecode = parameters.getOrElse("enableDecode", "true").toBoolean
    val spark = sqlContext.sparkSession
    ImageSchema.readImages(path = path,
      sparkSession = spark,
      recursive = recursive,
      sampleRatio = sampleRatio,
      dropImageFailures = dropImageFailures,
      repartitionNum = repartitionNum,
      numPartitions = numPartitions,
      filterByteSize = filterByteSize,
      enableDecode = enableDecode
    ).rdd
  }
}
