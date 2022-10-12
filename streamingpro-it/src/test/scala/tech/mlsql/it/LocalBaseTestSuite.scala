package tech.mlsql.it

import org.apache.commons.io.FileUtils
import org.apache.spark.streaming.SparkOperationUtil
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import serviceframework.dispatcher.StrategyDispatcher
import streaming.core.StreamingApp
import streaming.core.strategy.platform.{PlatformManager, SparkRuntime}
import tech.mlsql.job.JobManager

import java.io.File


trait LocalBaseTestSuite extends AnyFunSuite with SparkOperationUtil with BeforeAndAfterAll {

  var runtime: SparkRuntime = _
  var runParams: Array[String] = Array()
  var home: String = _
  val user = "admin"
  var initialPlugins: Seq[String] = Seq("mlsql-assert", "mlsql-shell", "mlsql-mllib")
  var originClassLoader: ClassLoader = Thread.currentThread().getContextClassLoader

  def getTestManager: TestManager = ???

  def setTestManager(_testManager: TestManager): Unit = ???

  def getTestCaseDirPath: Seq[String] = ???

  def setTestCaseDirPath(_testCaseDirPath: Seq[String]): Unit = ???

  def getDataDirPath: String = ???

  def setDataDirPath(_dataDirPath: String): Unit = ???

  def initPlugins(): Unit = {
    // 3.1.1 => v1=3 v2=1 v3=1
    val Array(v1, v2, _) = runtime.sparkSession.version.split("\\.")

    def convert_plugin_name(name: String) = {
      s"${name}-${v1}.${v2}"
    }
    // set plugin context loader
    Thread.currentThread().setContextClassLoader(ClassLoader.getSystemClassLoader)
    initialPlugins.foreach(name => {
      executeCode2(home, user, runtime, s"""!plugin app remove "${convert_plugin_name(name)}";""")
      executeCode2(home, user, runtime, s"""!plugin app add - "${convert_plugin_name(name)}";""")
    })

    executeCode2(
      home, user, runtime,
      s"""!plugin app remove "${convert_plugin_name("mlsql-excel")}";"""
    )
    executeCode2(
      home, user, runtime,
      s"""!plugin app add "tech.mlsql.plugins.ds.MLSQLExcelApp" "${convert_plugin_name("mlsql-excel")}";"""
    )
  }

  def setupWorkingDirectory(): Unit = {
    val homeDir = new File("src/test/home")
    if (!homeDir.exists()) {
      homeDir.mkdirs()
    }
    home = homeDir.getAbsolutePath
  }

  def copyDataToUserHome(user: String): Unit = {
    val userHomeDir = new File(home, user)
    val dataDir = new File(getDataDirPath)

    if (!userHomeDir.exists()) {
      userHomeDir.mkdirs()
    }
    FileUtils.copyDirectory(dataDir, userHomeDir)
  }

  override def beforeAll(): Unit = {
    setupWorkingDirectory()
    setupRunParams()
    copyDataToUserHome(user)
    getTestCaseDirPath.foreach(dirPath => {
      getTestManager.loadTestCase(new File(dirPath))
    })
    // Load built-in and external plug-ins and start PlatformManager
    StreamingApp.main(runParams)
    runtime = PlatformManager.getRuntime.asInstanceOf[SparkRuntime]
    JobManager.init(runtime.sparkSession)
    initPlugins()
  }

  override def afterAll(): Unit = {
    try {
      JobManager.shutdown
      StrategyDispatcher.clear
      PlatformManager.clear
      getTestManager.clear()
      Thread.currentThread().setContextClassLoader(originClassLoader)
      runtime.destroyRuntime(false, true)
      val db = new File("./metastore_db")
      if (db.exists()) {
        FileUtils.deleteDirectory(db)
      }
      val homeDir = new File(home)
      if (homeDir.exists()) {
        FileUtils.deleteDirectory(homeDir)
      }
    }
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  def setupRunParams(): Unit = {
    setTestCaseDirPath(Seq("src/test/resources/sql"))
    setDataDirPath("src/test/resources/data")
    setTestManager(new TestManager())
    runParams = Array(
      "-streaming.master", "local[*]",
      "-streaming.name", "unit-test",
      "-streaming.rest", "false",
      "-streaming.platform", "spark",
      "-streaming.enableHiveSupport", "false",
      "-streaming.spark.service", "false",
      "-streaming.datalake.path", "src/test/home/_delta_",
      "-streaming.unittest", "true"
    )
  }

  def runTestCases(): Unit = {
    getTestManager.testCases.foreach(testCase => {
      try {
        val result: Seq[Seq[String]] = executeCode2(home, user, runtime, testCase.sql)
        getTestManager.accept(testCase, result, null)
      } catch {
        case e: Exception =>
          getTestManager.accept(testCase, null, e)
      }
    })

    getTestManager.report()
  }

}

