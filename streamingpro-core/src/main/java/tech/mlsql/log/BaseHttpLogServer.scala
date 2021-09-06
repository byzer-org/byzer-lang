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

import org.apache.spark.{MLSQLSparkUtils, SparkEnv}
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.handler.AbstractHandler
import tech.mlsql.common.utils.distribute.socket.server.{Request, Response}
import tech.mlsql.common.utils.net.NetTool

trait BaseHttpLogServer extends AbstractHandler {

  var requestMapping: String
  var url: String = _
  var host: String = _
  var port: Int = _
  var server: Server = _

  def init(host: String, port: Int): (Server, String, String, Int)

  def build(host: String, port: Int): Boolean = {
    val (_server, _url, _host, _port) = init(host, port)
    this.server = _server
    this.url = _url
    this.host = _host
    this.port = _port
    server.isStarting || server.isStarted
  }

  def close(): Unit

  def getHost: String = {
    if (SparkEnv.get == null || MLSQLSparkUtils.rpcEnv().address == null) NetTool.localHostName()
    else MLSQLSparkUtils.rpcEnv().address.host
  }

}

case class SendLog(token: String, logLine: String) extends Request[LogRequest] {
  override def wrap = LogRequest(sendLog = this)
}

case class StopSendLog() extends Request[LogRequest] {
  override def wrap = LogRequest(stopSendLog = this)
}

case class Ok() extends Response[LogResponse] {
  override def wrap = LogResponse(ok = this)
}

case class Negative() extends Response[LogResponse] {
  override def wrap = LogResponse(negative = this)
}

case class LogResponse(
                        ok: Ok = null,
                        negative: Negative = null
                      ) {
  def unwrap: Response[_] = {
    if (ok != null) ok
    else if (negative != null) negative
    else null
  }
}

case class LogRequest(sendLog: SendLog = null, stopSendLog: StopSendLog = null) {
  def unwrap: Request[_] = {
    if (sendLog != null) sendLog
    else if (stopSendLog != null) stopSendLog
    else null
  }
}