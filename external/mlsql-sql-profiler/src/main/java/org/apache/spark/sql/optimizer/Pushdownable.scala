/*-
 * <<
 * Moonbox
 * ==
 * Copyright (C) 2016 - 2019 EDP
 * ==
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * >>
 */
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
package org.apache.spark.sql.optimizer

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Expression, ScalaUDF}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import org.apache.spark.sql.execution.aggregate.ScalaUDAF
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCRelation, JdbcRelationProvider}
import org.apache.spark.sql.sources.BaseRelation

trait Pushdownable { self: PushdownSourceInfo =>
  val supportedOperators: Set[Class[_]]
  val supportedJoinTypes: Set[JoinType]
  val supportedExpressions: Set[Class[_]]
  val beGoodAtOperators: Set[Class[_]]
  val supportedUDF: Set[String]

  def isGoodAt(operator: Class[_]): Boolean = {
    beGoodAtOperators.contains(operator)
  }

  def isSupportAll: Boolean

  def isSupport(lp: LogicalPlan): Boolean = {
    isSupportAll || {(lp match {
      case join: Join =>
        supportedOperators.contains(lp.getClass) && supportedJoinTypes.contains(join.joinType)
      case _ =>
        supportedOperators.contains(lp.getClass)
    }) && allExpressionSupport(lp)
    }
  }

  private def allExpressionSupport(lp: LogicalPlan): Boolean = {
    def traverseExpression(expression: Expression): Boolean ={
      expression match {
        case udf: ScalaUDF =>
          udf.udfName match {
            case Some(udfName) =>
              supportedExpressions.contains(udf.getClass) && supportedUDF.contains(udfName)
            case None => false
          }
        case udaf: ScalaUDAF => false
        case expr => supportedExpressions.contains(expr.getClass) && expression.children.forall(traverseExpression)
      }
    }
    lp.expressions.forall { expr =>
      traverseExpression(expr)
    }
  }

  def createRelationFromOld(querysql: String, oldRelation: BaseRelation): BaseRelation = {
    val options = oldRelation.asInstanceOf[JDBCRelation].jdbcOptions

    val parameters = (options.parameters - "dbtable") ++ Map("query"->querysql)

    new JdbcRelationProvider().createRelation(oldRelation.sqlContext, parameters)
  }

  def canPushdown(lp: LogicalPlan):Boolean = {
    false
  }

  def isPushDown():Boolean = {
    false
  }

  def fastEquals(other: PushdownSourceInfo): Boolean

  def buildScan(lp: LogicalPlan, sparkSession: SparkSession): DataFrame

  def buildScan(lp: LogicalPlan): DataFrame

  def buildScan2(lp: LogicalPlan, sparkSession: SparkSession): LogicalPlan

  def buildScan2(lp: LogicalPlan): LogicalPlan

}

