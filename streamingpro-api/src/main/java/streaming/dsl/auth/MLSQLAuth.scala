package streaming.dsl.auth

/**
  * Created by allwefantasy on 11/9/2018.
  */
trait MLSQLAuth {
  def isForbidden = false

  // ctx should be SQLContext; you should include
  // streamingpro-dsl or streamingpro-dsl-legcy which depends
  // your spark version
  def auth(_ctx: Any): TableAuthResult
}
