package streaming.test.stream

import org.apache.spark.streaming.BasicSparkOperation
import org.scalatest.BeforeAndAfterAll
import streaming.common.shell.ShellCommand
import streaming.core.strategy.platform.SparkRuntime
import streaming.core.{BasicMLSQLConfig, SpecFunctions}
import streaming.dsl.ScriptSQLExec
import streaming.log.Logging

class StreamSpec extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig with BeforeAndAfterAll with Logging {
  val KAFKA_0_8_HOME = System.getenv("KAFKA_0_8_HOME")

  val topic_name = "test_cool"
  "kafka8/kafka9" should "work fine on spark 2.3.x" in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      // we suppose that if KAFKA_HOME is configured ,then there must be a kafka server exists
      if (KAFKA_0_8_HOME != null) {

        ShellCommand.execCmd("rm -rf /tmp/cpl3")

        new Thread(new Runnable {
          override def run(): Unit = {
            var count = 30
            while (count > 0) {
              val ssel = createSSEL
              try {
                ScriptSQLExec.parse(
                  s"""
                     |select "a" as value as tmp_table1;
                     |save append tmp_table1
                     |as kafka8.`${topic_name}` where metadata.broker.list="127.0.0.1:9092";
                  """.stripMargin, ssel)
              } catch {
                case e: Exception => print(e.getMessage)
              }

              Thread.sleep(1000)
              count -= 1
            }

          }
        }).start()

        val ssel = createSSEL
        ScriptSQLExec.parse(
          s"""
             |set streamName="streamExample";
             |load kafka8.`${topic_name}` options kafka.bootstrap.servers="127.0.0.1:9092"
             |as table1;
             |
            |save append table1
             |as console.``
             |options mode="Append"
             |and duration="2"
             |and checkpointLocation="/tmp/cpl3";
          """.stripMargin, ssel)
        Thread.sleep(1000 * 30)
      }
      assume(spark.streams.active.size > 0)
      spark.streams.active.foreach(f => f.stop())
    }
  }

  val kafkaServer = new KafkaServer(Option(KAFKA_0_8_HOME), Seq(topic_name))

  override protected def beforeAll(): Unit = {
    if (KAFKA_0_8_HOME != null) {
      kafkaServer.startServer
    }
  }

  override protected def afterAll(): Unit = {
    if (KAFKA_0_8_HOME != null) {
      kafkaServer.shutdownServer
    }

  }
}
