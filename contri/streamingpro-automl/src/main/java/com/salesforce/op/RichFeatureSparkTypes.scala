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

package com.salesforce.op

import com.salesforce.op.features.FeatureSparkTypes
import com.salesforce.op.features.types.FeatureType

import scala.reflect.runtime.universe._
import scala.reflect.runtime.currentMirror
import scala.tools.reflect.ToolBox

/**
  * Created by allwefantasy on 22/9/2018.
  */
object RichFeatureSparkTypes {
  implicit def fromFeatureSparkTypes(objA: FeatureSparkTypes.type) = RichFeatureSparkTypes

  def featureTypeFromString[O](className: String): WeakTypeTag[_ <: FeatureType] = {
    createTypeTag(className)
  }

  private def createTypeTag(tp: String): WeakTypeTag[_ <: FeatureType] = {
    WeakTypeTag(currentMirror, new reflect.api.TypeCreator {
      def apply[U <: reflect.api.Universe with Singleton](m: reflect.api.Mirror[U]) = {
        val toolbox = currentMirror.mkToolBox()
        val ttagCall = s"scala.reflect.runtime.universe.weakTypeTag[$tp]"
        val tpe = toolbox.typecheck(toolbox.parse(ttagCall), toolbox.TYPEmode).tpe.resultType.typeArgs.head
        assert(m eq currentMirror, s"TypeTag[$tpe] defined in $currentMirror cannot be migrated to $m.")
        tpe.asInstanceOf[U#Type]
      }
    })
  }


}


