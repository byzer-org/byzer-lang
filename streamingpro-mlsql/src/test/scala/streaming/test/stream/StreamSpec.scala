package streaming.test.stream

import org.apache.spark.SparkCoreVersion
import org.apache.spark.streaming.BasicSparkOperation
import streaming.common.shell.ShellCommand
import streaming.core.strategy.platform.SparkRuntime
import streaming.core.{BasicMLSQLConfig, SpecFunctions}
import streaming.dsl.ScriptSQLExec

class StreamSpec extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig {
  "kafka8/kafka9" should "work fine on spark 2.3.x" in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      val kafkaHome = System.getenv("KAFKA_HOME")
      if (SparkCoreVersion.is_2_3_2() && kafkaHome != null) {
        val ssel = createSSEL
        ShellCommand.execCmd("rm -rf /tmp/cpl3")
        // check kafka if exists

        //        if (kafkaHome != null) {
        //          ShellCommand.execCmd("cd $KAFKA_HOME;./bin/kafka-server-start.sh  -daemon config/server.properties")
        //        }
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
}
