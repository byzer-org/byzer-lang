package streaming.test.stream

import org.apache.spark.SparkCoreVersion
import org.apache.spark.streaming.BasicSparkOperation
import org.scalatest.BeforeAndAfterAll
import streaming.common.shell.ShellCommand
import streaming.core.strategy.platform.SparkRuntime
import streaming.core.{BasicMLSQLConfig, SpecFunctions}
import streaming.dsl.ScriptSQLExec

class StreamSpec extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig with BeforeAndAfterAll {
  val KAFKA_0_8_HOME = System.getenv("KAFKA_0_8_HOME")

  "kafka8/kafka9" should "work fine on spark 2.3.x" in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      // we suppose that if KAFKA_HOME is configured ,then there must be a kafka server exists
      if (SparkCoreVersion.is_2_3_2() && KAFKA_0_8_HOME != null) {

        ShellCommand.execCmd("rm -rf /tmp/cpl3")

        new Thread(new Runnable {
          override def run(): Unit = {
            var count = 20
            while (count > 0) {
              val ssel = createSSEL
              ScriptSQLExec.parse(
                """
                  |select "a" as a as tmp_table1;
                  |save append tmp_table1
                  |as kafka8.`test_cool` where metadata.broker.list="127.0.0.1:9092";
                """.stripMargin, ssel)
              Thread.sleep(1000)
              count -= 1
            }

          }
        }).start()

        val ssel = createSSEL
        ScriptSQLExec.parse(
          """
            |set streamName="streamExample";
            |load kafka8.`` options kafka.bootstrap.servers="127.0.0.1:9092"
            |and `topics`="test_cool"
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
    }
  }

  override protected def beforeAll(): Unit = {
    if (KAFKA_0_8_HOME != null) {
      val res = ShellCommand.exec("cd $KAFKA_0_8_HOME;./bin/kafka-server-start.sh -daemon config/server.properties")
      println(res)
      Thread.sleep(20 * 1000)
    }
  }

  override protected def afterAll(): Unit = {
    if (KAFKA_0_8_HOME != null) {
      val res = ShellCommand.exec("cd $KAFKA_0_8_HOME;./bin/kafka-server-stop.sh")
      println(res)
    }

  }
}
