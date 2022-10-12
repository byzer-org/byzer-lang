package tech.mlsql.it

class SimpleQueryTestSuite extends LocalBaseTestSuite {

  private var testManager = new TestManager()
  private var dataDirPath = "src/test/resources/data"
  private var testCaseDirPath: Seq[String] = Seq("src/test/resources/sql/all_mode", "src/test/resources/sql/local_mode")

  override def getTestManager: TestManager = testManager

  override def getTestCaseDirPath: Seq[String] = testCaseDirPath

  override def getDataDirPath: String = dataDirPath

  override def setTestManager(_testManager: TestManager): Unit = testManager = _testManager

  override def setTestCaseDirPath(_testCaseDirPath: Seq[String]): Unit =
    testCaseDirPath = _testCaseDirPath

  override def setDataDirPath(_dataDirPath: String): Unit = dataDirPath = _dataDirPath

  override def setupRunParams(): Unit = {
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

  test("Simple Query Test") {
    runTestCases()
  }
}
