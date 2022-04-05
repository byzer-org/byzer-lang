package tech.mlsql.crawler

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import tech.mlsql.common.utils.log.Logging

class RestUtilsTest extends AnyFlatSpec with should.Matchers with Logging {

  "executeWithRetrying " should "run at most 3 times" in {
    var numExecuted = 0;
    RestUtils.executeWithRetrying[Int](3)(  {
      numExecuted += 1
      numExecuted
    }, _ => {
      false
    }, _ => {
      logInfo("Failing execution")
    })
    assert( numExecuted == 3 )
  }
}
