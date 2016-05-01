package streaming.core

import java.util.{List => JList, Map => JMap}


object LocalStreamingApp {


  def main(args: Array[String]): Unit = {
    StreamingApp.main(Array(
      "-streaming.master", "local[2]",
      "-streaming.duration", "30",
      "-streaming.name", "god",
      "-streaming.rest", "true"
    ))
  }

}
