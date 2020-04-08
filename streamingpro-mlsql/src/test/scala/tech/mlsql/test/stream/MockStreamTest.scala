package tech.mlsql.test.stream

import java.io.File

import org.apache.spark.sql.DataSetHelper
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.mock.MockStreamSource
import org.apache.spark.sql.streaming.util.StreamManualClock
import org.apache.spark.sql.streaming.{OutputMode, StreamTest, Trigger}
import org.apache.spark.sql.types.StructType
import org.scalatest.time.SpanSugar._
import tech.mlsql.common.utils.lang.sc.ScalaReflect

/**
  * 2019-08-12 WilliamZhu(allwefantasy@gmail.com)
  */
class MockStreamTest extends StreamTest {
  override val streamingTimeout = 6000.seconds

  def withTempDirs(f: (File, File) => Unit): Unit = {
    withTempDir { file1 =>
      withTempDir { file2 =>
        f(file1, file2)
      }
    }
  }

  object TriggerData {
    def apply(source: Source, f: () => Unit) = {
      new TriggerData(source) {
        override def addData(query: Option[StreamExecution]): (Source, Offset) = {
          f()
          (source, source.getOffset.get)
        }
      }
    }
  }


  abstract case class TriggerData(source: Source) extends AddData {

  }

  val sourceName = "org.apache.spark.sql.execution.streaming.mock.MockStreamSourceProvider"
  test("test mock stream") {
    failAfter(streamingTimeout) {
      withTempDirs { (outputDir, checkpointDir) =>

        //prepare data
        val items =
          """
            |{"key":"{\"a\":1}","value":"no","topic":"test","partition":0,"offset":0,"timestamp":"2008-01-24 18:01:01.001","timestampType":0}
            |{"key":"{\"a\":2}","value":"no","topic":"test","partition":0,"offset":1,"timestamp":"2008-01-24 18:01:01.002","timestampType":0}
            |{"key":"{\"a\":3}","value":"no","topic":"test","partition":0,"offset":2,"timestamp":"2008-01-24 18:01:01.003","timestampType":0}
            |{"key":"{\"a\":4}","value":"no","topic":"test","partition":0,"offset":3,"timestamp":"2008-01-24 18:01:01.003","timestampType":0}
            |{"key":"{\"a\":5}","value":"no","topic":"test","partition":0,"offset":4,"timestamp":"2008-01-24 18:01:01.003","timestampType":0}
            |{"key":"{\"a\":6}","value":"no","topic":"test","partition":0,"offset":5,"timestamp":"2008-01-24 18:01:01.003","timestampType":0}
            |
          """.stripMargin.split("\n").filterNot(_.trim.isEmpty)

        val session = spark
        import session.implicits._
        val data = spark.read.json(session.createDataset[String](items))
        data.createTempView("jack")

        val source = new MockStreamSource(spark.sqlContext, Map("path" -> "jack",
          "stepSizeRange" -> "0-3", "recycle" -> "true"
        ), checkpointDir.getPath)
        val attributes = ScalaReflect.fromInstance[StructType](source.schema).method("toAttributes").invoke().asInstanceOf[Seq[AttributeReference]]
        val logicalPlan = StreamingExecutionRelation(source, attributes)(sqlContext.sparkSession)
        val df = DataSetHelper.create(spark, logicalPlan)
        testStream(df, OutputMode.Append())(
          StartStream(Trigger.ProcessingTime("5 seconds"), new StreamManualClock),
          AdvanceManualClock(5 * 1000),
          CheckAnswerRowsByFunc(rows => {
            assert(rows.size < 4)
          }, true),
          AdvanceManualClock(5 * 1000),
          CheckAnswerRowsByFunc(rows => {
            assert(rows.size < 4)
          }, true),
          AdvanceManualClock(5 * 1000),
          CheckAnswerRowsByFunc(rows => {
            assert(rows.size < 4)
          }, true),
          AdvanceManualClock(5 * 1000),
          CheckAnswerRowsByFunc(rows => {
            assert(rows.size < 4)
          }, true)

        )

      }
    }
  }
}
