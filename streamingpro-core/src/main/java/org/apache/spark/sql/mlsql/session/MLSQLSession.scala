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

package org.apache.spark.sql.mlsql.session

import org.apache.spark.sql.SparkSession
import tech.mlsql.common.utils.log.Logging

import scala.collection.mutable.{HashSet => MHSet}


/**
  * Created by allwefantasy on 1/6/2018.
  */
class MLSQLSession(username: String,
                   password: String,
                   ipAddress: String,
                   withImpersonation: Boolean,
                   sessionManager: SessionManager,
                   opManager: MLSQLOperationManager,
                   sessionConf: Map[String, String] = Map()
                  ) extends Logging {


  @volatile private[this] var lastAccessTime: Long = System.currentTimeMillis()
  private[this] var lastIdleTime = 0L

  private[this] val activeOperationSet = new MHSet[String]()


  private[this] lazy val _mlsqlSparkSession = new MLSQLSparkSession(username, sessionConf)

  private[this] def acquire(userAccess: Boolean): Unit = {
    if (userAccess) {
      lastAccessTime = System.currentTimeMillis
    }
  }

  private[this] def release(userAccess: Boolean): Unit = {
    if (userAccess) {
      lastAccessTime = System.currentTimeMillis
    }
    if (activeOperationSet.isEmpty) {
      lastIdleTime = System.currentTimeMillis
    } else {
      lastIdleTime = 0
    }
  }

  def sparkSession: SparkSession = this._mlsqlSparkSession.sparkSession

  def mlsqlSparkSession: MLSQLSparkSession = this._mlsqlSparkSession

  def open(sessionConf: Map[String, String]): Unit = {
    mlsqlSparkSession.init(sessionConf)
    lastAccessTime = System.currentTimeMillis
    lastIdleTime = lastAccessTime
  }

  def close(): Unit = {
    acquire(true)
    try {
      // Iterate through the opHandles and close their operations
      activeOperationSet.clear()
    } finally {
      release(true)
    }
  }


  def visit(): MLSQLSession = {
    acquire(true)
    release(true)
    this
  }


  def getUserName = username

  def getOpManager = opManager

}
