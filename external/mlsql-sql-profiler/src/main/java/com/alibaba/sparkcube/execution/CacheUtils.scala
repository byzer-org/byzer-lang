/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.sparkcube.execution

import java.text.SimpleDateFormat

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, HiveTableRelation}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Cast, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.datasources.LogicalRelation

object CacheUtils {

  val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def existsIn(table: TableIdentifier, plan: LogicalPlan): Boolean = {
    plan.find {
      case LogicalRelation(_, _, Some(catalogTable), _) =>
        catalogTable.identifier.equals(table)
      case HiveTableRelation(catalogTable, _, _) =>
        catalogTable.identifier.equals(table)
      case _ => false
    }.nonEmpty
  }

  def aliasOutput(plan: LogicalPlan, expectedOutput: Seq[Attribute]): LogicalPlan = {
    assert(plan.output.length == expectedOutput.length)
    val alias = plan.output.zip(expectedOutput).map {
      pair =>
        Alias(pair._1, pair._2.name)(pair._2.exprId, pair._2.qualifier)
    }
    Project(alias, plan)
  }

  def findAttributeRelationInPlan(
      expr: Expression,
      plan: LogicalPlan): Option[(LogicalPlan, Expression)] = {
    expr match {
      case namedExpr: NamedExpression =>
        var currentExpr = namedExpr
        plan transformDown {
          case currentFragment =>
            currentFragment.transformExpressionsDown {
            case alias@ Alias(child, _) =>
              if (alias.exprId == namedExpr.exprId) {
                currentExpr = child match {
                  case ne: NamedExpression => ne
                  case Cast(grandchild, _, _) if (grandchild.isInstanceOf[NamedExpression]) =>
                    grandchild.asInstanceOf[NamedExpression]
                  case _ => currentExpr
                }
              }
              alias
          }
        }

        plan find {
          case PhysicalOperation(projects, _, _: LogicalRelation)
            if (projects.exists(currentExpr.semanticEquals(_))) => true
          case PhysicalOperation(projects, _, _: HiveTableRelation)
            if (projects.exists(currentExpr.semanticEquals(_))) => true
          case _ => false
        } map((_, currentExpr))
      case _ => None
    }
  }

  def getLeafTables(plan: LogicalPlan): Seq[CatalogTable] = {
    plan.collect {
      case LogicalRelation(_, _, Some(catalogTable), _) =>
        catalogTable
      case HiveTableRelation(catalogTable, _, _) =>
        catalogTable
    }
  }

  def formatTime(timestamp: Long): String = {
    dateFormat.format(timestamp)
  }
}
