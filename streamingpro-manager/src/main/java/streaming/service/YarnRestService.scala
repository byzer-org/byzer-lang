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

package streaming.service

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ConcurrentHashMap, Executors, TimeUnit}

import net.csdn.common.collections.WowCollections
import net.csdn.common.logging.Loggers
import net.csdn.common.settings.{ImmutableSettings, Settings}
import net.csdn.modules.threadpool.DefaultThreadPoolService
import net.csdn.modules.transport.{DefaultHttpTransportService, HttpTransportService}
import net.csdn.modules.transport.proxy.{AggregateRestClient, FirstMeetProxyStrategy}
import streaming.db.{DB, ManagerConfiguration, TSparkApplication, TSparkApplicationLog}
import streaming.remote.YarnApplicationState.YarnApplicationState
import streaming.remote.{YarnApplication, YarnController}
import streaming.shell.{AsyncShellCommand, Md5, ShellCommand}
import streaming.remote.YarnControllerE._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by allwefantasy on 12/7/2017.
  */



object YarnRestService {

  private val firstMeetProxyStrategy = new FirstMeetProxyStrategy()


  def yarnRestClient(hostAndPort: String): YarnController = AggregateRestClient.buildIfPresent[YarnController](hostAndPort, firstMeetProxyStrategy, RestClient.transportService)

  def query[T](resourceManagersStr: String, f: (YarnController, String) => T) = {
    val resourceManagers = resourceManagersStr.split(",").toList
    try {
      f(yarnRestClient(resourceManagers(0)), resourceManagers(0))
    }
    catch {
      case e: Exception =>
        if (resourceManagers.size > 1)
          f(yarnRestClient(resourceManagers(1)), resourceManagers(1))
        else null.asInstanceOf[T]
    }
  }

  def yarnApps(resourceManagers: String, states: List[YarnApplicationState]) = {
    val statesParam = states.map(state => state.toString).mkString(",")
    query[List[YarnApplication]](resourceManagers, (yarnController, _) =>
      yarnController.apps(statesParam).apps()
    )
  }

  def findApp(resourceManagers: String, appId: String) = {
    query[List[YarnApplication]](resourceManagers, (yarnController, _) =>
      yarnController.app(appId).app()
    )
  }

  def isRunning(resourceManagers: String, appId: String): Boolean = {
    val apps = findApp(resourceManagers, appId)
    isRunning(apps)
  }

  def isRunning(apps: List[YarnApplication]): Boolean = {
    apps.size > 0 && apps(0).state == YarnApplicationState.RUNNING.toString
  }

}

case class Task(taskId: String, host: String, appId: Long)

object YarnApplicationState extends Enumeration {
  type YarnApplicationState = Value
  val NEW = Value("NEW")
  val NEW_SAVING = Value("NEW_SAVING")
  val SUBMITTED = Value("SUBMITTED")
  val ACCEPTED = Value("ACCEPTED")
  val RUNNING = Value("RUNNING")
  val FINISHED = Value("FINISHED")
  val FAILED = Value("FAILED")
  val KILLED = Value("KILLED")
}

object RestClient {
  private final val settings: Settings = ImmutableSettings.settingsBuilder().loadFromClasspath("application.yml").build()
  final val transportService: HttpTransportService = new DefaultHttpTransportService(new DefaultThreadPoolService(settings), settings)
}

class RestClient

