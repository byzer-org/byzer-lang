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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, DecimalType, IntegralType}
import org.apache.spark.ui.UIUtils

/**
 * Access those package private functions
 */
object SparkAgent {

  def getConf(session: SparkSession): SQLConf = {
    session.sqlContext.conf
  }

  def getLogicalPlan(dataset: DataFrame): LogicalPlan = {
    dataset.logicalPlan
  }

  def createColumn(e: Expression): Column = {
    Column(e)
  }

  def formatDate(date: Long): String = {
    UIUtils.formatDate(date)
  }

  def getDataFrame(session: SparkSession, logicalPlan: LogicalPlan): DataFrame = {
    Dataset.ofRows(session, logicalPlan)
  }

  def getDataFrame(session: SparkSession, tableIdentifier: TableIdentifier): DataFrame = {
    session.table(tableIdentifier)
  }

  def analysisException(cause: String): AnalysisException = {
    new AnalysisException(cause)
  }

  def createDecimal(precision: Int, scale: Int): DecimalType = {
    DecimalType.bounded(precision, scale)
  }

  def isIntegral(dt: DataType): Boolean = {
    dt.isInstanceOf[IntegralType]
  }

  

  object DecimalResolve {
    def unapply(dt: DecimalType): Option[(Int, Int)] = dt match {
      case DecimalType.Fixed(precision, scale) => Some((precision, scale))
      case _ => None
    }
  }

}
