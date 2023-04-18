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

package tech.mlsql.log

import net.csdn.common.io.Streams

import java.util.concurrent.atomic.AtomicBoolean
import org.eclipse.jetty.server.{HttpConnectionFactory, Server, ServerConnector, Request => JettyRequest}
import org.eclipse.jetty.util.thread.QueuedThreadPool
import tech.mlsql.common.utils.base.TryTool
import tech.mlsql.common.utils.distribute.socket.server.Request
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.common.utils.net.NetTool
import tech.mlsql.common.utils.serder.json.JsonUtils

import java.io.InputStreamReader
import java.net.InetSocketAddress
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

/**
 * 2019-08-21 WilliamZhu(allwefantasy@gmail.com)
 */
class DriverLogServer(accessToken: String) extends BaseHttpLogServer with Logging {

  override var requestMapping: String = "/v2/writelog"
  @volatile private var markClose: AtomicBoolean = new AtomicBoolean(false)

  val threadPool = new QueuedThreadPool(100, 10, 60000)

  def init(host: String, port: Int): (Server, String, String, Int) = {

    val startService = (_port: Int) => {
      val server = new Server(threadPool)
      val connector = new ServerConnector(server, new HttpConnectionFactory())
      connector.setHost(host)
      connector.setPort(_port)
      connector.setAcceptQueueSize(100)
      server.addConnector(connector)
      server.setHandler(this)
      server.start()
      (server, server.getURI.getPort)
    }

    var server: Server = null
    var newPort = port
    TryTool.tryOrElse {
      // Check if it is the specified port
      if (port == 0) {
        val (_server, _newPort) = NetTool.startServiceOnPort[Server](0, startService, 10, "driver-log-server")
        server = _server
        newPort = _newPort
      } else {
        val (_server, _) = startService(port)
        server = _server
      }

    } {
      TryTool.tryOrNull {
        close()
        if (server != null && server.isRunning) {
          server.stop()
        }
      }
    }

    (server, s"http://$host:$newPort$requestMapping", host, newPort)
  }

  override def close(): Unit = {
    // Make sure we only close once
    if (markClose.compareAndSet(false, true)) {
      logInfo(s"Shutdown ${this.getServer.getURI}. This may caused by the task is killed.")
    }
  }

  override def handle(url: String,
                      baseRequest: JettyRequest,
                      request: HttpServletRequest,
                      response: HttpServletResponse): Unit = {
    request.setCharacterEncoding("UTF-8")
    url match {
      case x: String if url.contains(requestMapping) =>
        try {
          response.setStatus(HttpServletResponse.SC_OK)
          response.setContentType("application/json")
          val context = Streams.copyToString(new InputStreamReader(request.getInputStream, "UTF-8"))

          var logRequest: Any = null
          // We always set the complete sendLog in the key of the request parameters.
          if (context != null && context.nonEmpty) {
            logRequest = JsonUtils.fromJson[LogRequest](context).
              asInstanceOf[ {def unwrap: Request[_]}].unwrap
          }
          logRequest match {
            case SendLog(token, logLine) =>
              if (token != accessToken) {
                logInfo(s"$url auth fail. token:$token")
              } else {
                logInfo(logLine)
              }

            case _ =>
          }
        } catch {
          case e: Exception =>
            logError("DriverLogServer caught an exception when executing a handle. ", e)
        } finally {
          baseRequest.setHandled(true)
        }

      case _ =>
    }

  }
}