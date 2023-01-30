package tech.mlsql.tool

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.io.IOUtils
import org.apache.spark.MLSQLSparkUtils
import tech.mlsql.common.utils.Md5
import tech.mlsql.common.utils.path.PathFun

import java.io.{FileSystem => _, _}
import scala.collection.mutable.ArrayBuffer

/**
 * 11/11/2020 WilliamZhu(allwefantasy@gmail.com)
 */
object HDFSOperatorV2 {

  def hadoopConfiguration: Configuration = {
    if (MLSQLSparkUtils.sparkHadoopUtil != null) {
      MLSQLSparkUtils.sparkHadoopUtil.conf
    } else new Configuration()

  }

  def readFile(path: String): String = {
    val fs = new Path(path).getFileSystem(hadoopConfiguration)
    var br: BufferedReader = null
    var line: String = null
    val result = new ArrayBuffer[String]()
    try {
      br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))))
      line = br.readLine()
      while (line != null) {
        result += line
        line = br.readLine()
      }
    } finally {
      if (br != null) br.close()
    }
    result.mkString("\n")

  }

  def getFileStatus(path: String) = {
    val fs = new Path(path).getFileSystem(hadoopConfiguration)
    val file = fs.getFileStatus(new Path(path))
    file
  }

  def getContentSummary(path: String): ContentSummary = {
    val fs = new Path(path).getFileSystem(hadoopConfiguration)
    fs.getContentSummary(new Path(path))
  }

  def readAsInputStream(fileName: String): InputStream = {
    val src: Path = new Path(fileName)
    val fs = src.getFileSystem(hadoopConfiguration)
    var in: FSDataInputStream = null
    try {
      in = fs.open(src)
    } catch {
      case e: Exception =>
        if (in != null) in.close()
    }
    return in
  }


  def readBytes(fileName: String): Array[Byte] = {
    val src: Path = new Path(fileName)
    val fs = src.getFileSystem(hadoopConfiguration)
    var in: FSDataInputStream = null
    try {
      in = fs.open(src)
      val byteArrayOut = new ByteArrayOutputStream()
      IOUtils.copyBytes(in, byteArrayOut, 1024, true)
      byteArrayOut.toByteArray
    } finally {
      if (null != in) in.close()
    }
  }

  def listModelDirectory(path: String): Seq[FileStatus] = {
    val fs = new Path(path).getFileSystem(hadoopConfiguration)
    fs.listStatus(new Path(path)).filter(f => f.isDirectory)
  }

  def listFiles(path: String): Seq[FileStatus] = {
    val fs = new Path(path).getFileSystem(hadoopConfiguration)
    fs.listStatus(new Path(path))
  }

  def saveBytesFile(path: String, fileName: String, bytes: Array[Byte]) = {

    var dos: FSDataOutputStream = null
    try {

      val fs = new Path(path).getFileSystem(hadoopConfiguration)
      if (!fs.exists(new Path(path))) {
        fs.mkdirs(new Path(path))
      }
      dos = fs.create(new Path(new java.io.File(path, fileName).getPath), true)
      dos.write(bytes)
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

  def saveStream(path: String, fileName: String, inputStream: InputStream) = {

    var dos: FSDataOutputStream = null
    try {

      val fs = new Path(path).getFileSystem(hadoopConfiguration)
      if (!fs.exists(new Path(path))) {
        fs.mkdirs(new Path(path))
      }
      dos = fs.create(new Path(new java.io.File(path, fileName).getPath), true)
      IOUtils.copyBytes(inputStream, dos, 4 * 1024 * 1024)
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
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

  def ceateEmptyFile(path: String) = {
    val fs = new Path(path).getFileSystem(hadoopConfiguration)
    val dos = fs.create(new Path(path))
    dos.close()
  }

  def saveFile(path: String, fileName: String, iterator: Iterator[(String, String)]) = {

    var dos: FSDataOutputStream = null
    try {

      val fs = new Path(path).getFileSystem(hadoopConfiguration)
      if (!fs.exists(new Path(path))) {
        fs.mkdirs(new Path(path))
      }
      dos = fs.create(new Path(path, fileName), true)
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

  def getFilePath(path: String) = {
    new Path(path).toString
  }

  def copyToHDFS(tempLocalPath: String, path: String, cleanTarget: Boolean, cleanSource: Boolean) = {
    val fs = new Path(path).getFileSystem(hadoopConfiguration)
    if (cleanTarget) {
      fs.delete(new Path(path), true)
    }
    fs.copyFromLocalFile(new Path(tempLocalPath),
      new Path(path))
    if (cleanSource) {
      FileUtils.forceDelete(new File(tempLocalPath))
    }

  }

  def copyToLocalFile(tempLocalPath: String, path: String, clean: Boolean) = {
    val fs = new Path(path).getFileSystem(hadoopConfiguration)
    val tmpFile = new File(tempLocalPath)
    if (tmpFile.exists()) {
      FileUtils.forceDelete(tmpFile)
    }
    fs.copyToLocalFile(new Path(path), new Path(tempLocalPath))
  }

  def deleteDir(path: String) = {
    val fs = new Path(path).getFileSystem(hadoopConfiguration)
    fs.delete(new Path(path), true)
  }

  def isDir(path: String) = {
    val fs = new Path(path).getFileSystem(hadoopConfiguration)
    fs.isDirectory(new Path(path))
  }

  def isFile(path: String) = {
    val fs = new Path(path).getFileSystem(hadoopConfiguration)
    fs.isFile(new Path(path))
  }

  def fileExists(path: String) = {
    val fs = new Path(path).getFileSystem(hadoopConfiguration)
    fs.exists(new Path(path))
  }

  def createDir(path: String) = {
    val fs = new Path(path).getFileSystem(hadoopConfiguration)
    fs.mkdirs(new Path(path))
  }

  def createTempModelLocalPath(path: String, autoCreateParentDir: Boolean = true) = {
    val dir = PathFun.tmp.add("train").add(Md5.md5Hash(path)).toPath
    if (autoCreateParentDir) {
      FileUtils.forceMkdir(new File(dir))
    }
    dir
  }

  def iteratorFiles(path: String, recursive: Boolean) = {
    val fs = new Path(path).getFileSystem(hadoopConfiguration)
    val files = ArrayBuffer[String]()
    _iteratorFiles(fs, path, files)
    files
  }

  def _iteratorFiles(fs: FileSystem, path: String, files: ArrayBuffer[String]): Unit = {
    val p = new Path(path)
    val file = fs.getFileStatus(p)
    if (fs.exists(p)) {

      if (file.isFile) {
        files += p.toString
      }
      else if (file.isDirectory) {
        val fileStatusArr = fs.listStatus(p)
        if (fileStatusArr != null && fileStatusArr.length > 0) {
          for (tempFile <- fileStatusArr) {
            _iteratorFiles(fs, tempFile.getPath.toString, files)
          }
        }
      }
    }
  }

  def saveWithoutTopNLines(inPath: String, skipFirstNLines: Int, header: Boolean, encoding: String): String = {
    val fs = new Path(inPath).getFileSystem(hadoopConfiguration)
    val charset = encoding.trim.toLowerCase
    val src: Path = new Path(inPath)
    var br: BufferedReader = null
    var line: String = null
    var dos: FSDataOutputStream = null
    val pathElements = inPath.split(PathFun.pathSeparator)
    val writePathParts = pathElements.take(pathElements.length - 1) :+
      s"skipFirstNLines_${skipFirstNLines}_${charset}_${pathElements(pathElements.length - 1)}"
    val outPath = writePathParts.mkString(PathFun.pathSeparator)
    try {
      dos = fs.create(new Path(new java.io.File(outPath).getPath), true)
      br = new BufferedReader(new InputStreamReader(fs.open(src)))
      line = br.readLine()
      var count = 1
      while (line != null) {
        if (count > skipFirstNLines) {
          dos.writeBytes(line + "\n")
        }
        count += 1
        line = br.readLine()
      }
    } finally {
      if (br != null) br.close()
      if (null != dos) {
        try {
          dos.close()
        } catch {
          case ex: Exception => throw ex
        }
        dos.close()
      }
    }
    outPath
  }

  def saveWithoutLastNLines(inPath: String, skipLastNLines: Int): String = {
    val fs = new Path(inPath).getFileSystem(hadoopConfiguration)
    val src: Path = new Path(inPath)
    var line: String = null
    var dos: FSDataOutputStream = null
    var br1: BufferedReader = null
    var br2: BufferedReader = null
    val pathElements = inPath.split(PathFun.pathSeparator)
    val writePathParts = pathElements.take(pathElements.length - 1) :+ String.format("skipLastNLines_%s_%s", String.valueOf(skipLastNLines), pathElements(pathElements.length - 1))
    val outPath = writePathParts.mkString(PathFun.pathSeparator)

    try {
      dos = fs.create(new Path(new java.io.File(outPath).getPath), true)
      br1 = new BufferedReader(new InputStreamReader(fs.open(src)))
      var totalLinesCount = 0
      while (br1.readLine() != null) totalLinesCount += 1

      br2 = new BufferedReader(new InputStreamReader(fs.open(src)))
      line = br2.readLine()
      var count = 1

      while (line != null) {
        if (count <= totalLinesCount - skipLastNLines) {
          dos.writeBytes(line + "\n")
        }
        count += 1
        line = br2.readLine()
      }
    } finally {
      if (br1 != null) br1.close()
      if (null != dos) {
        try {
          dos.close()
        } catch {
          case ex: Exception => throw ex
        }
        dos.close()
      }
    }
    outPath
  }

}