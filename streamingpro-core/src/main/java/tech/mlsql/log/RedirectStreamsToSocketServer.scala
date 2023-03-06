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

import java.io.InputStream
import tech.mlsql.arrow.api.RedirectStreams

/**
 * 2019-08-21 WilliamZhu(allwefantasy@gmail.com)
 */
class RedirectStreamsToSocketServer extends RedirectStreams {
  var _conf: Map[String, String] = null

  override def setConf(conf: Map[String, String]): Unit = _conf = conf

  override def conf: Map[String, String] = _conf

  override def stdOut(stdOut: InputStream): Unit = {
    new RedirectLogThread(stdOut, conf, "stdout reader for logger server").start()
  }

  override def stdErr(stdErr: InputStream): Unit = {
    new RedirectLogThread(stdErr, conf, "stderr reader for logger server").start()
  }


}

class RedirectLogThread(
                         in: InputStream,
                         conf: Map[String, String],
                         name: String)
  extends Thread(name) {

  setDaemon(true)

  override def run(): Unit = {
    val pythonOutputEncoding = conf.getOrElse("pythonOutputEncoding","utf-8")
    WriteLog.write(scala.io.Source.fromInputStream(in,pythonOutputEncoding).getLines(), conf)
  }
}

object WriteLog extends BaseHttpLogClient
