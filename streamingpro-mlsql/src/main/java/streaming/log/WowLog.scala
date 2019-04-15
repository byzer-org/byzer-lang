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

package streaming.log

import streaming.dsl.ScriptSQLExec

import scala.collection.mutable.ArrayBuffer

/**
  * Created by allwefantasy on 4/9/2018.
  */
trait WowLog {
  def format(msg: String, skipPrefix: Boolean = false) = {
    if (skipPrefix) {
      msg
    } else {
      if (ScriptSQLExec.context() != null) {
        val context = ScriptSQLExec.context()
        s"""[owner] [${context.owner}] [groupId] [${context.groupId}] $msg"""
      } else {
        s"""[owner] [null] [groupId] [null] $msg"""
      }
    }


  }

  def wow_format(msg: String) = {
    format(msg)

  }

  def format_exception(e: Exception) = {
    (e.toString.split("\n") ++ e.getStackTrace.map(f => f.toString)).map(f => format(f)).toSeq.mkString("\n")
  }

  def format_throwable(e: Throwable, skipPrefix: Boolean = false) = {
    (e.toString.split("\n") ++ e.getStackTrace.map(f => f.toString)).map(f => format(f,skipPrefix)).toSeq.mkString("\n")
  }

  def format_cause(e: Exception) = {
    var cause = e.asInstanceOf[Throwable]
    while (cause.getCause != null) {
      cause = cause.getCause
    }
    format_throwable(cause)
  }

  def format_full_exception(buffer: ArrayBuffer[String], e: Exception, skipPrefix: Boolean = true) = {
    var cause = e.asInstanceOf[Throwable]
    buffer += format_throwable(cause,skipPrefix)
    while (cause.getCause != null) {
      cause = cause.getCause
      buffer += "caused by：\n" + format_throwable(cause,skipPrefix)
    }

  }
}
