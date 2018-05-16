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
