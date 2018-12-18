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

package streaming.core.shared.pool

/**
  * Created by allwefantasy on 21/5/2018.
  */
class ForestPool[Any] extends BigObjPool[Any] {
  val map = new java.util.concurrent.ConcurrentHashMap[String, Any]()

  override def size(): Int = map.size()

  override def get(name: String): Any = map.get(name)

  override def put(name: String, value: Any): BigObjPool[Any] = {
    map.put(name, value)
    this
  }

  override def remove(name: String): BigObjPool[Any] = {
    map.remove(name)
    this
  }

  def clear = {
    map.clear()
  }
}
