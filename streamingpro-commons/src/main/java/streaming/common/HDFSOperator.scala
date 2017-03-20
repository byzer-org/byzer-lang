package streaming.common

import java.io.{BufferedReader, InputStreamReader}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}

import scala.collection.mutable.ArrayBuffer

/**
  * 5/5/16 WilliamZhu(allwefantasy@gmail.com)
  */
object HDFSOperator {

  def readFile(path: String): String = {
    val fs = FileSystem.get(new Configuration())
    var br: BufferedReader = null
    var line: String = null
    val result = new ArrayBuffer[String]()
    try {
      br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))))
      line = br.readLine()
      while (line != null) {
        line = br.readLine()
        result += line
      }
    } finally {
      if (br != null) br.close()
    }
    result.mkString("\n")

  }


  def saveFile(path: String, fileName: String, iterator: Iterator[(String, String)]) = {

    var dos: FSDataOutputStream = null
    try {

      val fs = FileSystem.get(new Configuration())
      if (!fs.exists(new Path(path))) {
        fs.mkdirs(new Path(path))
      }
      dos = fs.create(new Path(path + s"/$fileName"), true)
      iterator.foreach { x =>
        dos.writeBytes(x._2 + "\n")
      }
    } catch {
      case ex: Exception =>
        println("file save exception")
    } finally {
      if (null != dos) {
        try {
          dos.close()
        } catch {
          case ex: Exception =>
            println("close exception")
        }
        dos.close()
      }
    }

  }
}
