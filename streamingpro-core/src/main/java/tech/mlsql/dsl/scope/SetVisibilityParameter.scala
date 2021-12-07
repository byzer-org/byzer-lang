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

package tech.mlsql.dsl.scope

import tech.mlsql.dsl.scope.ParameterVisibility.ParameterVisibility

import scala.collection.mutable

/**
  * Created by aston on 5/9/2019.
  */

case class SetVisibilityParameter(value: String, scope: mutable.Set[ParameterVisibility])

object ParameterVisibility extends Enumeration {
  type ParameterVisibility = Value
  val UN_SELECT = Value("un_select")
  val ALL = Value("all")
}

object SetVisibilityOption extends Enumeration {
  val NAME="visible"
}


