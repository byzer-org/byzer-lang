package tech.mlsql.it

class SimpleQueryTestSuite extends LocalBaseTestSuite {

  override def getTestManager = new TestManager()

  override def getTestCaseDirPath: Seq[String] = Seq("src/test/resources/sql/all_mode")

  override def getDataDirPath = "src/test/resources/data"

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
