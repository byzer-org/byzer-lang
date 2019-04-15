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

package streaming.core.shared

import java.util.concurrent.Executors

import streaming.core.shared.pool.{ForestPool, BigObjPool, DicPool}

/**
  * Created by allwefantasy on 21/5/2018.
  */
class SharedObjManager {
  //private[this] val _executor = Executors.newFixedThreadPool(1)
}

object SharedObjManager {
  val forestPool = new ForestPool[Any]()

  def getOrCreate[T](name: String, bigObjPool: BigObjPool[T], func: () => T) = {
    synchronized {
      if (bigObjPool.get(name) == null) {
        bigObjPool.put(name, func())
      }
      bigObjPool.get(name)
    }
  }

  def clear = {
    forestPool.clear
  }
}
