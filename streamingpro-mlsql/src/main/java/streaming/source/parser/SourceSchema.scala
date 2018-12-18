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

package streaming.source.parser

import org.apache.spark.sql.types.WowStructType
import streaming.parser.SparkTypePaser

/**
  * Created by allwefantasy on 14/9/2018.
  */
case class SourceSchema(_schemaStr: String) {
  private val sparkSchema = SparkTypePaser.cleanSparkSchema(SparkTypePaser.toSparkSchema(_schemaStr).asInstanceOf[WowStructType])

  def schema = sparkSchema

  def schemaStr = _schemaStr
}
