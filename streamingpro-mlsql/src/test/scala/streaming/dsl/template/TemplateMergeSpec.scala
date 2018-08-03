package streaming.dsl.template

import org.scalatest.{FlatSpec, Matchers}
import streaming.core.NotToRunTag

class TemplateMergeSpec extends FlatSpec with Matchers {


  "template merge" should "work" taggedAs (NotToRunTag) in {
    val sql = TemplateMerge.merge("${yesterday}",Map[String,String]("yesterday"->"2017-03-01"))
    assume(sql == "2017-03-01")
  }

}
