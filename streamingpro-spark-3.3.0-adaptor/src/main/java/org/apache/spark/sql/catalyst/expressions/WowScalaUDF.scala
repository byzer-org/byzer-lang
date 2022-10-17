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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.types.DataType
import tech.mlsql.common.utils.log.Logging

import scala.util.Properties

case class WowScalaUDF(function: AnyRef,
                       dataType: DataType,
                       children: Seq[Expression],
                       inputsNullSafe: Seq[Boolean],
                       inputTypes: Seq[DataType] = Nil,
                       udfName: Option[String] = None,
                       nullable: Boolean = true,
                       udfDeterministic: Boolean = true) {

  def this(
            function: AnyRef,
            dataType: DataType,
            children: Seq[Expression]
          ) = {
    this(
      function, dataType, children, WowScalaUDF.getParameterTypeNullability(function),
      Nil, None, nullable = true, udfDeterministic = true)
  }


  def toScalaUDF = {

    new ScalaUDF(function,
      dataType,
      children,
      Nil,
      None,
      udfName,
      nullable,
      udfDeterministic
    )
  }

}

object WowScalaUDF extends Logging {
  def getParameterTypeNullability(func: AnyRef): Seq[Boolean] = {
    if (!Properties.versionString.contains("2.11")) {
      logWarning(s"Scala ${Properties.versionString} cannot get type nullability correctly via " +
        "reflection, thus Spark cannot add proper input null check for UDF. To avoid this " +
        "problem, use the typed UDF interfaces instead.")
    }
    val methods = func.getClass.getMethods.filter(m => m.getName == "apply" && !m.isBridge)
    assert(methods.length == 1)
    methods.head.getParameterTypes.map(!_.isPrimitive)
  }
}

