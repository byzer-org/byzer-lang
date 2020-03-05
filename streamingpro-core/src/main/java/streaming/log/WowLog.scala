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

import scala.collection.mutable.ArrayBuffer

/**
  * Created by allwefantasy on 4/9/2018.
  */
trait WowLog {

  import tech.mlsql.log.LogUtils

  def format(msg: String, skipPrefix: Boolean = false) = {
    LogUtils.format(msg, skipPrefix)
  }

  def wow_format(msg: String) = {
    LogUtils.format(msg)
  }

  def format_exception(e: Exception) = {
    LogUtils.format_exception(e)
  }

  def format_throwable(e: Throwable, skipPrefix: Boolean = false) = {
    LogUtils.format_throwable(e, skipPrefix)
  }

  def format_cause(e: Exception) = {
    LogUtils.format_cause(e)
  }

  def format_full_exception(buffer: ArrayBuffer[String], e: Exception, skipPrefix: Boolean = true) = {
    LogUtils.format_full_exception(buffer, e, skipPrefix)
  }
}
