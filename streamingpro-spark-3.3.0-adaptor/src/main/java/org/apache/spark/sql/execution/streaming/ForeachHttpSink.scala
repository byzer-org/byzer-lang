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

package org.apache.spark.sql.execution.streaming

import net.csdn.modules.http.RestResponse
import org.apache.spark.internal.Logging
import org.apache.spark.sql._

/**
  * Created by allwefantasy on 28/3/2017.
  */
class ForeachHttpSink(writer: RestResponse) extends Sink with Logging {


  override def addBatch(batchId: Long, data: DataFrame): Unit = synchronized {
    val result = data.sparkSession.createDataFrame(
      data.sparkSession.sparkContext.parallelize(data.collect()), data.schema).toJSON.collect().mkString("")
    writer.write(result)
  }
}
