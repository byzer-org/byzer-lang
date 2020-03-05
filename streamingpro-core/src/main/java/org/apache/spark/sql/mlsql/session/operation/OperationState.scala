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

package org.apache.spark.sql.mlsql.session.operation

import org.apache.spark.sql.mlsql.session.MLSQLException


/**
  * Created by allwefantasy on 4/6/2018.
  */
trait OperationState {
  def toTOperationState(): String

  def isTerminal(): Boolean = false

  def validateTransition(newState: OperationState): Unit = ex(newState)

  protected def ex(state: OperationState): Unit = throw new MLSQLException(
    "Illegal Operation state transition " + this + " -> " + state, "ServerError", 1000)
}

case object INITIALIZED extends OperationState {
  override def toTOperationState(): String = "INITIALIZED_STATE"

  override def validateTransition(newState: OperationState): Unit = newState match {
    case PENDING | RUNNING | CANCELED | CLOSED =>
    case _ => ex(newState)
  }
}

case object RUNNING extends OperationState {
  override def toTOperationState(): String = "RUNNING_STATE"

  override def validateTransition(newState: OperationState): Unit = newState match {
    case FINISHED | CANCELED | ERROR | CLOSED =>
    case _ => ex(newState)
  }
}

case object FINISHED extends OperationState {
  override def toTOperationState(): String = "FINISHED_STATE"

  override def isTerminal(): Boolean = true

  override def validateTransition(newState: OperationState): Unit = newState match {
    case CLOSED =>
    case _ => ex(newState)
  }
}

case object CANCELED extends OperationState {
  override def toTOperationState(): String = "CANCELED_STATE"

  override def isTerminal(): Boolean = true

  override def validateTransition(newState: OperationState): Unit = newState match {
    case CLOSED =>
    case _ => ex(newState)
  }
}

case object CLOSED extends OperationState {
  override def toTOperationState(): String = "CLOSED_STATE"

  override def isTerminal(): Boolean = true
}

case object ERROR extends OperationState {
  override def toTOperationState(): String = "ERROR_STATE"

  override def isTerminal(): Boolean = true

  override def validateTransition(newState: OperationState): Unit = newState match {
    case CLOSED =>
    case _ => ex(newState)
  }
}

case object UNKNOWN extends OperationState {
  override def toTOperationState(): String = "UKNOWN_STATE"
}

case object PENDING extends OperationState {
  override def toTOperationState(): String = "PENDING_STATE"

  override def validateTransition(newState: OperationState): Unit = newState match {
    case RUNNING | FINISHED | CANCELED | ERROR | CLOSED =>
    case _ => ex(newState)
  }
}
