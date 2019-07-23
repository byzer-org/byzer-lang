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

package org.apache.spark.streaming

import java.io.File
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference

import net.csdn.common.reflect.ReflectHelper
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Row
import org.scalatest.{FlatSpec, Matchers}
import serviceframework.dispatcher.{Compositor, StrategyDispatcher}
import streaming.common.ParamsUtil
import streaming.core.strategy.platform.{PlatformManager, SparkRuntime}
import streaming.dsl.{MLSQLExecuteContext, ScriptSQLExec, ScriptSQLExecListener}
import tech.mlsql.ets.ScriptRunner
import tech.mlsql.job.{JobManager, MLSQLJobInfo, MLSQLJobProgress, MLSQLJobType}

/**
  * Created by allwefantasy on 30/3/2017.
  */
trait BasicSparkOperation extends FlatSpec with Matchers {

  def waitJobStarted(groupId: String, timeoutSec: Long = 10) = {
    var count = timeoutSec
    while (JobManager.getJobInfo.filter(f => f._1 == groupId) == 0 && count > 0) {
      Thread.sleep(1000)
      count -= 1
    }
    count > 0
  }

  def waitJobStartedByName(jobName: String, timeoutSec: Long = 10) = {
    var count = timeoutSec
    while (JobManager.getJobInfo.filter(f => f._2.jobName == jobName) == 0 && count > 0) {
      Thread.sleep(1000)
      count -= 1
    }
    count > 0
  }

  def waitWithCondition(shouldWait: () => Boolean, timeoutSec: Long = 10) = {
    var count = timeoutSec
    while (shouldWait() && count > 0) {
      Thread.sleep(1000)
      count -= 1
    }
    count > 0
  }

  def checkJob(runtime: SparkRuntime, groupId: String) = {
    val items = executeCode(runtime,
      """
        |!show jobs;
      """.stripMargin)
    items.filter(r => r.getAs[String]("groupId") == groupId).length == 1
  }

  def getSessionByOwner(runtime: SparkRuntime, owner: String) = {
    runtime.asInstanceOf[SparkRuntime].getSession(owner)
  }

  def getSession(runtime: SparkRuntime) = {
    runtime.asInstanceOf[SparkRuntime].getSession("william")
  }

  def autoGenerateContext(runtime: SparkRuntime, name: String = "william", groupId: String = UUID.randomUUID().toString, userDefinedParams: Map[String, String] = Map()) = {
    val exec = new ScriptSQLExecListener(getSessionByOwner(runtime, name), s"/tmp/${name}", Map())
    val context = MLSQLExecuteContext(exec, name, s"/tmp/${name}", groupId, userDefinedParams)
    context.execListener.addEnv("SKIP_AUTH", "true")
    ScriptSQLExec.setContext(context)
  }

  def createJobInfoFromExistGroupId(code: String) = {
    val context = ScriptSQLExec.context()
    val startTime = System.currentTimeMillis()
    MLSQLJobInfo(context.owner, MLSQLJobType.SCRIPT, UUID.randomUUID().toString, code, context.groupId, new MLSQLJobProgress(0, 0), startTime, -1)
  }

  def executeCodeWithGroupId(runtime: SparkRuntime, groupId: AtomicReference[String], code: String) = {
    autoGenerateContext(runtime)
    groupId.set(ScriptSQLExec.context().groupId)
    val jobInfo = createJobInfoFromExistGroupId(code)
    val holder = new AtomicReference[Array[Row]]()
    ScriptRunner.runJob(code, jobInfo, (df) => {
      holder.set(df.take(100))
    })
    holder.get()
  }

  def executeStreamCode(runtime: SparkRuntime, code: String) = {
    autoGenerateContext(runtime)
    val jobInfo = createJobInfoFromExistGroupId(code)
    val holder = new AtomicReference[Array[Row]]()
    ScriptRunner.runJob(code, jobInfo, (df) => {
      holder.set(df.take(100))
    })
    holder.get()
  }

  def executeCodeWithGroupIdAsync(runtime: SparkRuntime, groupId: AtomicReference[String], code: String): Option[Array[Row]] = {
    new Thread(new Runnable {
      override def run(): Unit = {
        executeCodeWithGroupId(runtime, groupId, code)
      }
    }).start()
    None
  }

  def executeCode(runtime: SparkRuntime, code: String) = {
    autoGenerateContext(runtime)
    val jobInfo = createJobInfoFromExistGroupId(code)
    val holder = new AtomicReference[Array[Row]]()
    ScriptRunner.runJob(code, jobInfo, (df) => {
      holder.set(df.take(100))
    })
    holder.get()

  }

  def executeCodeAsync(runtime: SparkRuntime, code: String, async: Boolean = false): Option[Array[Row]] = {
    new Thread(new Runnable {
      override def run(): Unit = {
        executeCode(runtime, code)
      }
    }).start()
    None
  }

  def withContext[R](runtime: SparkRuntime)(block: SparkRuntime => R): R = {
    try {
      JobManager.init(runtime.sparkSession)
      block(runtime)
    } finally {
      try {
        JobManager.shutdown
        StrategyDispatcher.clear
        PlatformManager.clear
        runtime.destroyRuntime(false, true)
        val db = new File("./metastore_db")
        FileUtils.deleteQuietly(new File("/tmp/william"))
        if (db.exists()) {
          FileUtils.deleteDirectory(db)
        }
      } catch {
        case e: Exception =>
          e.printStackTrace()
      }
    }
  }

  def withBatchContext[R](runtime: SparkRuntime)(block: SparkRuntime => R): R = {
    try {
      block(runtime)
    } finally {
      try {
        StrategyDispatcher.clear
        PlatformManager.clear
        runtime.destroyRuntime(false, true)
        val db = new File("./metastore_db")
        if (db.exists()) {
          FileUtils.deleteDirectory(db)
        }
      } catch {
        case e: Exception =>
          e.printStackTrace()
      }
    }
  }

  def getCompositorParam(item: Compositor[_]) = {
    ReflectHelper.field(item, "_configParams").
      asInstanceOf[java.util.List[java.util.Map[Any, Any]]]
  }

  def setupBatchContext(batchParams: Array[String], configFilePath: String = null) = {
    var params: ParamsUtil = null
    if (configFilePath != null) {
      val extraParam = Array("-streaming.job.file.path", configFilePath)
      params = new ParamsUtil(batchParams ++ extraParam)
    } else {
      params = new ParamsUtil(batchParams)
    }
    PlatformManager.getOrCreate.run(params, false)
    val runtime = PlatformManager.getRuntime.asInstanceOf[SparkRuntime]
    runtime
  }

  def appWithBatchContext(batchParams: Array[String], configFilePath: String) = {
    var runtime: SparkRuntime = null
    try {
      val extraParam = Array("-streaming.job.file.path", configFilePath)
      val params = new ParamsUtil(batchParams ++ extraParam)
      PlatformManager.getOrCreate.run(params, false)
      runtime = PlatformManager.getRuntime.asInstanceOf[SparkRuntime]
    } finally {
      try {
        StrategyDispatcher.clear
        PlatformManager.clear
        if (runtime != null) {
          runtime.destroyRuntime(false, true)
        }
        FileUtils.deleteDirectory(new File("./metastore_db"))
      } catch {
        case e: Exception =>
          e.printStackTrace()
      }
    }
  }


}
