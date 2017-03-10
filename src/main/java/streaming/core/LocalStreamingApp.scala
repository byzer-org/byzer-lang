package streaming.core

import java.util.{List => JList, Map => JMap}

import net.csdn.ServiceFramwork
import net.csdn.bootstrap.Application


object LocalStreamingApp {


  def main(args: Array[String]): Unit = {
    StreamingApp.main(Array(
      "-streaming.master", "local[2]",
      "-streaming.duration", "10",
      "-spark.sql.shuffle.partitions","1",
      "-streaming.name", "god",
      "-streaming.rest", "false"
      ,"-streaming.driver.port","9902",
      "-streaming.platform", "spark_streaming",
      "-streaming.sql.out.jack.user","root",
      "-streaming.sql.out.jack.password","csdn.net",
      "-streaming.sql.out.jack.url","jdbc:mysql://127.0.0.1/alarm_test?characterEncoding=utf8"
      //"-streaming.enableCarbonDataSupport", "true",
      //"-streaming.carbondata.store", "/tmp/carbondata/store"
      //"-streaming.carbondata.meta", "/tmp/carbondata/meta"
      //, "-spark.deploy.zookeeper.url","127.0.0.1"
      //, "-streaming.checkpoint","file:///tmp/ss"
     // ,"-streaming.testinputstream.offsetPath", "hdfs://cdn237:8020/tmp/localstreampingapp"
     // ,"-streaming.spark.hadoop.fs.defaultFS","hdfs://cdn237:8020"
    ))
  }

}

object AM {


  def main(args: Array[String]): Unit = {
    ServiceFramwork.scanService.setLoader(classOf[StreamingApp])
    Application.main(Array())
  }

}
