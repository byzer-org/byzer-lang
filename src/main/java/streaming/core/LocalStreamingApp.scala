package streaming.core

import java.util.{List => JList, Map => JMap}

import net.csdn.ServiceFramwork
import net.csdn.bootstrap.Application


object LocalStreamingApp {


  def main(args: Array[String]): Unit = {
    StreamingApp.main(Array(
      "-streaming.master", "local[2]",
      "-streaming.duration", "20",
      "-streaming.name", "god",
      "-streaming.rest", "false"
     // ,"-streaming.job.file.path","hdfs://cdn237:8020/tmp/bb.yml"
      ,"-streaming.driver.port","9902"
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
