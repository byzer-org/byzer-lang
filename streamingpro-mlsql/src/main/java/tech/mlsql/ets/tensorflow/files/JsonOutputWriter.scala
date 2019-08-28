package tech.mlsql.ets.tensorflow.files

import java.io.{File, FileWriter}

import org.apache.hadoop.mapreduce._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.execution.datasources.json.WowJsonInferSchema
import org.apache.spark.sql.types.StructType

class JsonOutputWriter(path: String, schema: StructType, sessionLocalTimeZone: String)
  extends OutputWriter {


  private val recordWriter: RecordWriter[Void, InternalRow] = {
    new RecordWriter[Void, InternalRow]() {
      val fileWriter = new FileWriter(new File(path))

      val (gen, writer) = WowJsonInferSchema.getJsonGen(schema, sessionLocalTimeZone)

      override def write(key: Void, value: InternalRow): Unit = {
        gen.write(value)
        gen.flush()
        val json = writer.toString
        fileWriter.write(json + "\n")
        writer.reset()
      }

      override def close(context: TaskAttemptContext): Unit = {
        gen.close()
      }
    }
  }

  override def write(row: InternalRow): Unit = recordWriter.write(null, row)

  override def close(): Unit = recordWriter.close(null)
}
