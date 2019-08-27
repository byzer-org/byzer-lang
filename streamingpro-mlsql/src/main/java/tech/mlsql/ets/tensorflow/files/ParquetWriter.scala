package tech.mlsql.ets.tensorflow.files

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce._
import org.apache.parquet.hadoop.ParquetOutputFormat
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.OutputWriter

class ParquetOutputWriter(path: String, conf: Configuration)
  extends OutputWriter {

  private val recordWriter: RecordWriter[Void, InternalRow] = {
    new ParquetOutputFormat[InternalRow]().getRecordWriter(conf, new Path(path), CompressionCodecName.SNAPPY)
  }

  override def write(row: InternalRow): Unit = recordWriter.write(null, row)

  override def close(): Unit = recordWriter.close(null)
}
