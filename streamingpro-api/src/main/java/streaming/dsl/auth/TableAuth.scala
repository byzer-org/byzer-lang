package streaming.dsl.auth

/**
  * Created by allwefantasy on 11/9/2018.
  */
trait TableAuth {
  def auth(table: List[MLSQLTable]): List[TableAuthResult]
}
