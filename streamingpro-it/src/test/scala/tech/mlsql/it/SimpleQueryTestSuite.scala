package tech.mlsql.it

class SimpleQueryTestSuite extends LocalBaseTestSuite {

  override def setupRunParams(): Unit = {
    testCaseDirPath = "src/test/resources/sql/simple"
    dataDirPath = "src/test/resources/data/simple"
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
