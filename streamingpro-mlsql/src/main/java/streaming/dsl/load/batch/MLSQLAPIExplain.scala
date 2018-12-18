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

package streaming.dsl.load.batch

import net.csdn.ServiceFramwork
import net.csdn.api.controller.APIDescAC
import net.csdn.common.settings.Settings
import net.sf.json.{JSONArray, JSONObject}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConversions._

/**
  * 2018-11-30 WilliamZhu(allwefantasy@gmail.com)
  */
class MLSQLAPIExplain(sparkSession: SparkSession) extends SelfExplain {
  override def isMatch: Boolean = false

  override def explain: DataFrame = {
    val items = JSONArray.fromObject(APIDescAC.openAPIs(ServiceFramwork.injector.getInstance(classOf[Settings]))).
      flatMap(f => f.asInstanceOf[JSONObject].getJSONArray("actions").map(m => JSONObject.fromObject(m).toString))
    val rows = sparkSession.sparkContext.parallelize(items, 1)
    sparkSession.read.json(rows)
  }
}
