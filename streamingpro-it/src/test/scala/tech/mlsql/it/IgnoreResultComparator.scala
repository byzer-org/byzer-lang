package tech.mlsql.it

class IgnoreResultComparator extends DefaultComparator {
  override def compareResult(testCase: TestCase, result: Seq[Seq[String]]): (Boolean, String) = {
    (true, "")
  }
}