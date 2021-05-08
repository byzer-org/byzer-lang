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
package org.apache.spark.sql.optimizer

import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation

abstract class PushdownSourceInfo(props: Map[String, String]){

}

object PushdownSourceInfo {

  //TODO : 用注册的方式

  def getPushdownSourceInfo(lr:LogicalRelation): PushdownSourceInfo ={
    lr.relation match {
      case l: JDBCRelation if (l.jdbcOptions.url.toLowerCase.startsWith("jdbc:mysql:")) =>
        val x= l.jdbcOptions.parameters.toMap
        new MysqlPushdownSourceInfo(l.jdbcOptions.parameters,l.sparkSession,lr)
      case l: JDBCRelation if (l.jdbcOptions.url.toLowerCase.startsWith("jdbc:kylin:")) =>
        new KylinPushdownSourceInfo(l.jdbcOptions.parameters,l.sparkSession,lr)
      case l: JDBCRelation if (l.jdbcOptions.url.toLowerCase.startsWith("jdbc:h2:")) =>
        new H2PushdownSourceInfo(l.jdbcOptions.parameters,l.sparkSession,lr)
      case _ =>
        new NoPushdownSourceInfo(Map())
    }

  }

  def registSourceInfo():Unit= {
  }

}

class NoPushdownSourceInfo(props: Map[String, String]) extends PushdownSourceInfo(props){}
