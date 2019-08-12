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

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.sql.SparkSession
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.job.JobManager

/**
  * Created by allwefantasy on 1/6/2018.
  */
class SessionManager(rootSparkSession: SparkSession) extends Logging {

  private[this] val identifierToSession = new ConcurrentHashMap[SessionIdentifier, MLSQLSession]
  private[this] var shutdown: Boolean = false
  private[this] val opManager = new MLSQLOperationManager(60)


  def start(): Unit = {
    SparkSessionCacheManager.setSessionManager(this)
    SparkSessionCacheManager.startCacheManager()
  }

  def stop(): Unit = {
    shutdown = true
    SparkSessionCacheManager.get.stop()
  }

  def openSession(
                   username: String,
                   password: String,
                   ipAddress: String,
                   sessionConf: Map[String, String],
                   withImpersonation: Boolean): SessionIdentifier = {

    val session = new MLSQLSession(
      username,
      password,
      ipAddress,
      withImpersonation,
      this, opManager
    )
    logInfo(s"Opening session for $username")
    session.open(sessionConf)

    identifierToSession.put(SessionIdentifier(username), session)
    SessionIdentifier(username)
  }

  def getSession(sessionIdentifier: SessionIdentifier): MLSQLSession = {
    synchronized {
      var session = identifierToSession.get(sessionIdentifier)
      if (session == null) {
        openSession(sessionIdentifier.owner, "", "", Map(), true)
      }
      session = identifierToSession.get(sessionIdentifier)
      //to record last visit timestamp
      SparkSessionCacheManager.get.visit(session.getUserName)
      //to record active times
      session.visit()
    }
  }

  def getSessionOption(sessionIdentifier: SessionIdentifier): Option[MLSQLSession] = {
    val session = getSession(sessionIdentifier)
    if (session == null) None else Some(session)
  }

  def closeSession(sessionIdentifier: SessionIdentifier) {
    val runningJobCnt = JobManager.getJobInfo
      .filter(_._2.owner == sessionIdentifier.owner)
      .size

    if (runningJobCnt == 0) {
      val session = identifierToSession.remove(sessionIdentifier)
      if (session == null) {
        throw new MLSQLException(s"Session $sessionIdentifier does not exist!")
      }
      session.close()
    } else {
      SparkSessionCacheManager.get.visit(sessionIdentifier.owner)
      logInfo(s"Session can't close ,$runningJobCnt jobs are running")
    }
  }

  def getOpenSessionCount: Int = identifierToSession.size
}


