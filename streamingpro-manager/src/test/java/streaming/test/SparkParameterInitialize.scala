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

package streaming.test

import net.sf.json.{JSONArray, JSONObject}
import streaming.bean.DeployParameterService
import streaming.common.ParamsUtil
import streaming.db.{DB, ManagerConfiguration, TSparkJobParameter, TSparkJobParameterValues}

import scala.io.Source

/**
  * Created by allwefantasy on 14/7/2017.
  */
object SparkParameterInitialize {
  ManagerConfiguration.config = new ParamsUtil(Array(
    "-jdbcPath", "/tmp/jdbc.properties",
    "-yarnUrl", "",
    "-afterLogCheckTimeout", "5",
    "-enableScheduler", "false"
  ))

  DB

  def main(args: Array[String]): Unit = {
//    DeployParameterService.installSteps("spark").foreach { f =>
//      val item = new TSparkJobParameter(-1,
//        f.name,
//        f.parentName,
//        f.parameterType,
//        f.app,
//        f.desc,
//        f.label,
//        f.priority,
//        f.formType,
//        f.actionType,
//        f.comment,
//        f.value
//      )
//      //TSparkJobParameter.save(item)
//
//    }
//    val str = Source.fromInputStream(this.getClass.getResourceAsStream("/sparkParameter.json")).getLines().mkString("\n")
//    import scala.collection.JavaConversions._
//    JSONObject.fromObject(str).foreach {
//      f =>
//        val item = (f._1.asInstanceOf[String], f._2.asInstanceOf[JSONArray].map(f => f.asInstanceOf[String]).toList)
//        val temp = new TSparkJobParameterValues(-1, item._1, 1, item._2.mkString(","))
//        TSparkJobParameterValues.save(new TSparkJobParameterValues(-1, item._1, 0, item._2.mkString(",")))
//
//    }

  }
}
