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

package streaming.core.strategy

import java.util

/**
 * 12/17/14 WilliamZhu(allwefantasy@gmail.com)
 */
trait DebugTrait {

  def putDebug(params: util.Map[Any, Any],key:String,value:AnyRef) = {
    putDebug2(params,this.toString,key,value)
  }

  def putDebug2(params: util.Map[Any, Any],classKey:String,key:String,value:AnyRef) = {

    if(debugEnable(params)){
      val debugInfo = params.get("_debug_").asInstanceOf[util.Map[String,AnyRef]]
      if(!debugInfo.containsKey(classKey)){
        debugInfo.put(classKey,new util.HashMap[String,AnyRef]())
      }
      debugInfo.get(classKey).asInstanceOf[util.Map[String,AnyRef]].put(key,value)
    }
  }

  def changeDebugData(stragetyParams: java.util.Map[Any, Any]): Map[Any, Any] = {

    val debugData = stragetyParams.get("_debug_").asInstanceOf[util.Map[Any, Any]]
    debugData.keySet().toArray.map{
      f=>
        val temp =  debugData.get(f).asInstanceOf[util.HashMap[String, AnyRef]]

        (f, temp.keySet().toArray.map(k=> (k, temp.get(k))).toMap)
    }.toMap
  }


  def debugEnable(params: util.Map[Any, Any]) = {
    params.containsKey("debug") && params.get("debug").asInstanceOf[String].toBoolean
  }
}
