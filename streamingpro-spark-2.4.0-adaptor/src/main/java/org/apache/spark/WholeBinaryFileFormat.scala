/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark {
  package input {

    import com.google.common.io.{ByteStreams, Closeables}
    import org.apache.hadoop.conf.Configuration
    import org.apache.hadoop.io.compress.CompressionCodecFactory
    import org.apache.hadoop.io.{BytesWritable, Text}
    import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit
    import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}

    /**
      * A [[org.apache.hadoop.mapreduce.RecordReader RecordReader]] for reading a single whole binary file
      * out in a key-value pair, where the key is the file path and the value is the entire content of
      * the file.
      */
    private[spark] class WholeBinaryFileRecordReader(
                                                      split: CombineFileSplit,
                                                      context: TaskAttemptContext,
                                                      index: Integer)
      extends RecordReader[Text, BytesWritable] with Configurable {

      private[this] val path = split.getPath(index)
      private[this] val fs = path.getFileSystem(context.getConfiguration)

      // True means the current file has been processed, then skip it.
      private[this] var processed = false

      private[this] val key: Text = new Text(path.toString)
      private[this] var value: BytesWritable = null

      override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {}

      override def close(): Unit = {}

      override def getProgress: Float = if (processed) 1.0f else 0.0f

      override def getCurrentKey: Text = key

      override def getCurrentValue: BytesWritable = value

      override def nextKeyValue(): Boolean = {
        if (!processed) {
          val conf = new Configuration
          val factory = new CompressionCodecFactory(conf)
          val codec = factory.getCodec(path) // infers from file ext.
          val fileIn = fs.open(path)
          val innerBuffer = if (codec != null) {
            ByteStreams.toByteArray(codec.createInputStream(fileIn))
          } else {
            ByteStreams.toByteArray(fileIn)
          }

          value = new BytesWritable(innerBuffer)
          Closeables.close(fileIn, false)
          processed = true
          true
        } else {
          false
        }
      }
    }

  }

  package sql.execution.datasources {

    import java.io.Closeable
    import java.net.URI

    import org.apache.hadoop.conf.Configuration
    import org.apache.hadoop.fs.Path
    import org.apache.hadoop.io.BytesWritable
    import org.apache.hadoop.mapreduce._
    import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit
    import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
    import org.apache.spark.input.WholeBinaryFileRecordReader

    /**
      * An adaptor from a [[PartitionedFile]] to an [[Iterator]] of [[BytesWritable]], which is all of the lines
      * in that file.
      */
    class HadoopFileWholeBinaryReader(file: PartitionedFile, conf: Configuration)
      extends Iterator[BytesWritable] with Closeable {
      private val iterator = {
        val fileSplit = new CombineFileSplit(
          Array(new Path(new URI(file.filePath))),
          Array(file.start),
          Array(file.length),
          Array.empty[String])
        val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
        val hadoopAttemptContext = new TaskAttemptContextImpl(conf, attemptId)
        //use customed WholeBinaryFileRecordReader
        val reader = new WholeBinaryFileRecordReader(fileSplit, hadoopAttemptContext, 0)
        reader.initialize(fileSplit, hadoopAttemptContext)
        new RecordReaderIterator(reader)
      }

      override def hasNext: Boolean = iterator.hasNext

      override def next(): BytesWritable = iterator.next()

      override def close(): Unit = iterator.close()
    }


    package binary {

      import java.io.OutputStream

      import org.apache.hadoop.conf.Configuration
      import org.apache.hadoop.fs.{FileStatus, Path}
      import org.apache.hadoop.mapreduce.Job
      import org.apache.spark.broadcast.Broadcast
      import org.apache.spark.sql.catalyst.InternalRow
      import org.apache.spark.sql.catalyst.expressions.UnsafeRow
      import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
      import org.apache.spark.sql.sources._
      import org.apache.spark.sql.types.{BinaryType, DataType, StringType, StructType}
      import org.apache.spark.sql.{AnalysisException, SparkSession}
      import org.apache.spark.unsafe.types.UTF8String
      import org.apache.spark.util.SerializableConfiguration

      /**
        * A data source for reading whole binary files.
        */
      class WholeBinaryFileFormat extends FileFormat with DataSourceRegister {

        override def shortName(): String = "wholeBinary"

        override def toString: String = "wholeBinary"

        private def verifySchema(schema: StructType): Unit = {
          if (schema.size != 2) {
            throw new AnalysisException(
              s"WholeBinary data source supports only two column, and you have ${schema.size} columns.")
          }
          val pathType = schema(0).dataType
          val contentType = schema(1).dataType
          if (pathType != StringType || contentType != BinaryType)
            throw new AnalysisException(
              s"WholeBinary data source supports only a string column indexed 0 as path and a binary column indexed 1 as content, but you have ${pathType.simpleString} and ${contentType.simpleString}.")
        }

        override def isSplitable(
                                  sparkSession: SparkSession,
                                  options: Map[String, String],
                                  path: Path): Boolean = {
          //whole binary file not be splitable
          false
        }

        override def inferSchema(
                                  sparkSession: SparkSession,
                                  options: Map[String, String],
                                  files: Seq[FileStatus]): Option[StructType] = {
          //define schema
          Some(new StructType().add("path", StringType).add("content", BinaryType))
        }

        override def buildReader(
                                  sparkSession: SparkSession,
                                  dataSchema: StructType,
                                  partitionSchema: StructType,
                                  requiredSchema: StructType,
                                  filters: Seq[Filter],
                                  options: Map[String, String],
                                  hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
          assert(
            requiredSchema.length <= 2,
            "WholeBinary data source only produces two data column named \"path\" and \"content\".")
          val broadcastedHadoopConf =
            sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

          readToUnsafeMem(broadcastedHadoopConf, requiredSchema)
        }

        private def readToUnsafeMem(
                                     conf: Broadcast[SerializableConfiguration],
                                     requiredSchema: StructType): (PartitionedFile) => Iterator[UnsafeRow] = {

          (file: PartitionedFile) => {
            val confValue = conf.value.value
            //use customed HadoopFileWholeBinaryReader
            val reader = new HadoopFileWholeBinaryReader(file, confValue)
            Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => reader.close()))
            if (requiredSchema.isEmpty) {
              val emptyUnsafeRow = new UnsafeRow(0)
              reader.map(_ => emptyUnsafeRow)
            } else {
              val unsafeRowWriter = new UnsafeRowWriter(2)

              reader.map { line =>
                // Writes to an UnsafeRow directly
                unsafeRowWriter.reset()
                unsafeRowWriter.write(0, UTF8String.fromString(file.filePath))
                unsafeRowWriter.write(1, line.getBytes, 0, line.getLength)
                unsafeRowWriter.getRow()
              }
            }
          }
        }

        override def supportDataType(dataType: DataType, isReadPath: Boolean): Boolean = {
          //define supported columns type
          dataType == StringType || dataType == BinaryType
        }

        override def prepareWrite(sparkSession: SparkSession, job: Job, options: Map[String, String], dataSchema: StructType): OutputWriterFactory = {
          new OutputWriterFactory {
            override def newInstance(
                                      path: String,
                                      dataSchema: StructType,
                                      context: TaskAttemptContext): OutputWriter = {
              new WholeBinaryOutputWriter(path, dataSchema, context)
            }

            override def getFileExtension(context: TaskAttemptContext): String = {
              ""
            }
          }
        }
      }

      class WholeBinaryOutputWriter(
                                     path: String,
                                     dataSchema: StructType,
                                     context: TaskAttemptContext)
        extends OutputWriter {

        override def write(row: InternalRow): Unit = {
          val originFile = new Path(path)

          //Overwrite the file name generated by the system with a custom file name
          val fileName = row.getString(0)
          val realFile = new Path(originFile.getParent, fileName)

          val fs = realFile.getFileSystem(context.getConfiguration)
          val writer: OutputStream = fs.create(realFile, false)
          try {
            if (!row.isNullAt(0)) {
              val content = row.getBinary(1)
              writer.write(content)
              writer.flush()
            }
          } finally {
            if (null != writer)
              writer.close()
          }
        }

        override def close(): Unit = {
        }
      }

    }

  }

}


