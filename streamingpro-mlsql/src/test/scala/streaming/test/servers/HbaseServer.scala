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

package streaming.test.servers

/**
  * Created by latincross on 12/28/2018.
  */
class HbaseServer(version: String) extends WowBaseTestServer {

  override def composeYaml: String =
    s"""
       |version: '2.1'
       |services:
       |  hbase:
       |    image: harisekhon/hbase:${version}
       |    ports:
       |      - "2181:2181"
       |      - "8080:8080"
       |      - "8085:8085"
       |      - "9090:9090"
       |      - "9095:9095"
       |      - "16000:16000"
       |      - "16010:16010"
       |      - "16201:16201"
       |      - "16301:16301"
       |    hostname: hbase-docker
    """.stripMargin

  override def waitToServiceReady: Boolean = {
    // wait mongo to ready, runs on host server
    val shellCommand = s"exec hbase shell <<EOF \n list_namespace \nEOF\n"
    readyCheck("", shellCommand, false)
  }
}
