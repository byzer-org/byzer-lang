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

import java.sql.SQLException

/**
  * Created by allwefantasy on 3/6/2018.
  */
class MLSQLException(reason: String, sqlState: String, vendorCode: Int, cause: Throwable) extends SQLException(reason, sqlState, vendorCode, cause) {
  def this(reason: String, sqlState: String, cause: Throwable) = this(reason, sqlState, 0, cause)


  def this(reason: String, sqlState: String, vendorCode: Int) =
    this(reason, sqlState, vendorCode, null)

  def this(reason: String, cause: Throwable) = this(reason, null, 0, cause)

  def this(reason: String, sqlState: String) = this(reason, sqlState, vendorCode = 0)

  def this(reason: String) = this(reason, sqlState = null, vendorCode = 0)

  def this(cause: Throwable) = this(reason = null, cause)
}
