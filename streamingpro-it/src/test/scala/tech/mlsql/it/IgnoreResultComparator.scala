package tech.mlsql.it

class IgnoreResultComparator extends Comparator {
  var baseComparator: Comparator = DefaultComparator

  override def of(_baseComparator: Comparator): Unit = {
    baseComparator = _baseComparator
  }

  override def compareResult(testCase: TestCase, result: TestResult): (Boolean, String) = {
    (true, "")
  }

  override def compareException(testCase: TestCase, testResult: TestResult): (Boolean, String) =
    baseComparator.compareException(testCase, testResult)

}