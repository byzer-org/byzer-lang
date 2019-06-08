package tech.mlsql.test.delta

import java.io.File

import org.apache.spark.SparkCoreVersion
import org.apache.spark.sql.delta.actions.{Action, AddFile, RemoveFile}
import org.apache.spark.sql.delta.commands.CompactTableInDelta
import org.apache.spark.sql.delta.{DeltaLog, DeltaOptions}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming._
import org.scalatest.time.SpanSugar._
import streaming.dsl.mmlib.Core_2_3_x

// scalastyle:off: removeFile
class DeltaCompactionSuite extends StreamTest {

  override val streamingTimeout = 1800.seconds

  import testImplicits._

  private def withTempDirs(f: (File, File) => Unit): Unit = {
    withTempDir { file1 =>
      withTempDir { file2 =>
        f(file1, file2)
      }
    }
  }

  test("append mode") {
    if (SparkCoreVersion.version > Core_2_3_x.coreVersion) {
      failAfter(streamingTimeout) {
        withTempDirs { (outputDir, checkpointDir) =>
          val inputData = MemoryStream[Int]
          val df = inputData.toDF()
          val query = df.writeStream
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .outputMode(OutputMode.Append())
            .format("delta")
            .start(outputDir.getCanonicalPath)
          val log = DeltaLog.forTable(spark, outputDir.getCanonicalPath)
          try {
            (1 to 15).foreach { i =>
              inputData.addData(i)
              query.processAllAvailable()
            }
            val writeThread = new Thread(new Runnable {
              override def run(): Unit =
                (1 to 15).foreach { i =>
                  inputData.addData(i)
                  Thread.sleep(1000)
                  query.processAllAvailable()
                }
            })
            writeThread.start()
            val optimizeTableInDelta = CompactTableInDelta(log,
              new DeltaOptions(Map[String, String](), df.sparkSession.sessionState.conf), Seq(), Map(
                CompactTableInDelta.COMPACT_VERSION_OPTION -> "8",
                CompactTableInDelta.COMPACT_NUM_FILE_PER_DIR -> "1",
                CompactTableInDelta.COMPACT_RETRY_TIMES_FOR_LOCK -> "60"
              ))
            optimizeTableInDelta.run(df.sparkSession)
            writeThread.join()
            val fileNum = new File(outputDir.getCanonicalPath).listFiles().
              filter(f => f.getName.endsWith(".parquet")).length

            val outputDf = spark.read.format("delta").load(outputDir.getCanonicalPath)
            assert(outputDf.count() == 30)
            assert(fileNum == 22)


          } finally {
            query.stop()
          }
        }
      }
    }

  }
  test("append mode with partitions") {
    if (SparkCoreVersion.version > Core_2_3_x.coreVersion) {
      failAfter(streamingTimeout) {
        withTempDirs { (outputDir, checkpointDir) =>
          val inputData = MemoryStream[A]
          val df = inputData.toDF()
          val query = df.writeStream
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .outputMode(OutputMode.Append())
            .format("delta")
            .partitionBy("key")
            .start(outputDir.getCanonicalPath)
          val log = DeltaLog.forTable(spark, outputDir.getCanonicalPath)
          try {
            (1 to 15).foreach { i =>
              val a = if (i > 3) "jack" else "william"
              inputData.addData(A(a, i))
              query.processAllAvailable()
            }
            val writeThread = new Thread(new Runnable {
              override def run(): Unit =
                (1 to 15).foreach { i =>
                  val a = if (i > 4) "jack" else "william"
                  inputData.addData(A(a, i))
                  Thread.sleep(1000)
                  query.processAllAvailable()
                }
            })
            writeThread.start()
            val optimizeTableInDelta = CompactTableInDelta(log,
              new DeltaOptions(Map[String, String](), df.sparkSession.sessionState.conf), Seq(), Map(
                CompactTableInDelta.COMPACT_VERSION_OPTION -> "8",
                CompactTableInDelta.COMPACT_NUM_FILE_PER_DIR -> "1",
                CompactTableInDelta.COMPACT_RETRY_TIMES_FOR_LOCK -> "60"
              ))
            val items = optimizeTableInDelta.run(df.sparkSession)
            writeThread.join()

            def recursiveListFiles(f: File): Array[File] = {
              val these = f.listFiles
              these ++ these.filter(_.isDirectory).flatMap(recursiveListFiles)
            }


            val fileNum = recursiveListFiles(new File(outputDir.getCanonicalPath)).filter { f =>
              f.getName.endsWith(".parquet") && !f.getName.contains("checkpoint")
            }.length


            val acitons = items.map(f => Action.fromJson(f.getString(0)))
            val newFilesSize = acitons.filter(f => f.isInstanceOf[AddFile]).size
            val removeFilesSize = acitons.filter(f => f.isInstanceOf[RemoveFile]).size

            val outputDf = spark.read.format("delta").load(outputDir.getCanonicalPath)
            assert(outputDf.count() == 30)
            assert(fileNum == (30 - removeFilesSize + newFilesSize))


          } finally {
            query.stop()
          }
        }
      }
    }

  }
}

case class A(key: String, value: Int)
