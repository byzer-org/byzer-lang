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

package streaming.dsl.auth.client

import java.util.concurrent.atomic.AtomicReference

import streaming.dsl.ScriptSQLExec
import streaming.dsl.auth.{MLSQLTable, TableAuth, TableAuthResult}
import streaming.log.WowLog
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.common.utils.serder.json.JSONTool

/**
 * Created by allwefantasy on 11/9/2018.
 */
class DefaultConsoleClient extends TableAuth with Logging with WowLog {
  override def auth(tables: List[MLSQLTable]): List[TableAuthResult] = {
    val owner = ScriptSQLExec.contextGetOrForTest().owner
    logInfo(format(s"auth ${owner}  want access tables: ${JSONTool.toJsonStr(tables)}"))
    DefaultConsoleClient.set(tables)
    List(TableAuthResult(true, ""))
  }
}

// for testing only
object DefaultConsoleClient {
  private val value = new AtomicReference[List[MLSQLTable]]()

  def set(tables: List[MLSQLTable]) = {
    value.set(tables)
  }

  def get = {
    value.get()
  }
}
