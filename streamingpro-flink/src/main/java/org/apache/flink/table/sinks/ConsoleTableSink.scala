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

package org.apache.flink.table.sinks

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.types.Row

/**
  * Created by allwefantasy on 20/3/2017.
  */
class ConsoleTableSink(_num: Int = 10) extends TableSinkBase[Row] with AppendStreamTableSink[Row] {


  override def emitDataStream(dataStream: DataStream[Row]): Unit = {
    dataStream.print()
  }


  override protected def copy: TableSinkBase[Row] = {
    new ConsoleTableSink(_num)
  }

  override def getOutputType: TypeInformation[Row] = {
    new RowTypeInfo(getFieldTypes: _*)
  }


}
