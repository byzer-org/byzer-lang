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

package streaming.common

import net.sf.json.{JSONArray, JSONObject}
import tech.mlsql.common.utils.serder.json.JSONTool

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable

/**
  * Created by allwefantasy on 31/7/2018.
  */
class TimeRecord(debug: Boolean) {
  private val list = new ArrayBuffer[DurationItem]()
  private var lastPunchItem: Option[PunchItem] = None
  private var children = new ArrayBuffer[TimeRecord]()
  private var parent: Option[TimeRecord] = None

  def stepIn(async: Boolean = false) = {
    if (!debug) {
      null
    } else {
      synchronized {
        val tr = new TimeRecord(debug)
        tr.parent = Some(this)
        children += tr
        val v = TimeRecord.stack.get()
        if (v == null) {
          TimeRecord.setStack(new mutable.Stack[TimeRecordWrapper]())
        }
        TimeRecord.stack.get().push(TimeRecordWrapper(async, tr))
        tr
      }
    }

  }

  def stepOut = {
    //this.parent.get
  }

  def renew = {
    if (!debug) {} else {
      lastPunchItem = Some(PunchItem(System.currentTimeMillis()))

    }
    this
  }

  def punch(name: String) = {
    if (!debug) {}
    else {
      val time = System.currentTimeMillis()
      lastPunchItem match {
        case Some(pi) =>
          list += DurationItem(name, time - pi.time)
        case None =>
      }
      lastPunchItem = Some(PunchItem(time))
    }
    this

  }
}

object TimeRecord {
  val stack = new ThreadLocal[mutable.Stack[TimeRecordWrapper]]()

  def setStack(sk: mutable.Stack[TimeRecordWrapper]) = {
    stack.set(sk)
  }

  def unsetStack = {
    stack.remove()
  }


  def begin(debug: Boolean) = {
    if (stack.get() == null || stack.get().isEmpty) {
      new TimeRecord(debug)
    } else {
      val trW = stack.get().pop()
      trW.timeRecord
    }
  }

  def mixin(res: String, root: TimeRecord) = {

    TimeRecord.unsetStack

    def f(t: TimeRecord): ResultItem = {
      val name = t.parent match {
        case Some(i) => i.list.last.name
        case None => ""
      }
      ResultItem(name, t.list.toList, t.children.map(tr => f(tr)).toList)
    }
    val time_debug = JSONTool.toJsonStr(f(root))
    val newres = new JSONObject()
    val tmp = try {
      JSONArray.fromObject(res)
    } catch {
      case e: Exception =>
        JSONObject.fromObject(res)
    }
    newres.put("data", tmp)
    newres.put("time_debug", time_debug)
    newres.toString()
  }

}

case class TimeRecordWrapper(async: Boolean, timeRecord: TimeRecord)

case class PunchItem(time: Long)

case class DurationItem(name: String, time: Long)

case class ResultItem(name: String, di: List[DurationItem], children: List[ResultItem])
