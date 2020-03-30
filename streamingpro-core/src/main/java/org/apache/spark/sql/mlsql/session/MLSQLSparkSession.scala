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

import java.lang.reflect.UndeclaredThrowableException

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import streaming.core.strategy.platform.{PlatformManager, SparkRuntime}
import streaming.core.stream.MLSQLStreamManager
import tech.mlsql.common.utils.log.Logging

import scala.collection.mutable.{HashSet => MHSet}


/**
 * Created by allwefantasy on 3/6/2018.
 */
class MLSQLSparkSession(userName: String, conf: Map[String, String]) extends Logging {
  private[this] var _sparkSession: SparkSession = _

  def sparkSession: SparkSession = _sparkSession


  private[this] def getOrCreate(sessionConf: Map[String, String]): Unit = synchronized {
    var checkRound = 15
    val interval = 1000L
    // if user's sc is being constructed by another
    while (!PlatformManager.RUNTIME_IS_READY.get() || MLSQLSparkSession.isPartiallyConstructed(userName)) {
      wait(interval)
      checkRound -= 1
      if (checkRound <= 0) {
        throw new MLSQLException(s"A partially constructed SparkContext for [$userName] " +
          s"has last more than ${checkRound * interval} seconds." +
          s"RUNTIME_IS_READY:${PlatformManager.RUNTIME_IS_READY.get()} " +
          s"isPartiallyConstructed:${MLSQLSparkSession.isPartiallyConstructed(userName)}")
      }
      logInfo(s"A partially constructed SparkContext for [$userName], $checkRound times countdown.")
    }

    SparkSessionCacheManager.get.getAndIncrease(userName) match {
      case Some(ss) =>
        _sparkSession = ss
      case _ =>
        MLSQLSparkSession.setPartiallyConstructed(userName)
        notifyAll()
        create(sessionConf)
    }
    MLSQLStreamManager.start(_sparkSession)
  }


  private[this] def create(sessionConf: Map[String, String]): Unit = {
    logInfo(s"--------- Create new SparkSession for $userName ----------")
    try {
      _sparkSession = PlatformManager.getRuntime.asInstanceOf[SparkRuntime].sparkSession.cloneSession()
      SparkSessionCacheManager.get.set(userName, _sparkSession)
    } catch {
      case e: Exception =>
        throw new MLSQLException(
          s"Get SparkSession for [$userName] failed: " + e, "", e)
    } finally {
      MLSQLSparkSession.setFullyConstructed(userName)
    }
  }

  def isLocalMaster(conf: SparkConf): Boolean = {
    val master = conf.get("spark.master", "")
    master == "local" || master.startsWith("local[")
  }


  def init(sessionConf: Map[String, String]): Unit = {
    try {
      getOrCreate(sessionConf)
    } catch {
      case ute: UndeclaredThrowableException => throw ute.getCause
      case e: Exception => throw e
    }
  }
}

object MLSQLSparkSession extends Logging {
  private[this] val userSparkContextBeingConstruct = new MHSet[String]()

  def setPartiallyConstructed(user: String): Unit = {
    userSparkContextBeingConstruct.add(user)
  }

  def isPartiallyConstructed(user: String): Boolean = {
    userSparkContextBeingConstruct.contains(user)
  }

  def setFullyConstructed(user: String): Unit = {
    userSparkContextBeingConstruct.remove(user)
  }

  def cloneSession(session: SparkSession) = {
    session.cloneSession()
  }

}