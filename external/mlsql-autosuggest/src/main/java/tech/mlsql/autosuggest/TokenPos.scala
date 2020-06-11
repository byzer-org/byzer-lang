package tech.mlsql.autosuggest

/**
 * TokenPos  mark the cursor position
 */
object TokenPosType {
  val END = -1
  val CURRENT = -2
  val NEXT = -3
}

/**
 * input "load" the cursor can be (situation 1) in the end of load
 * or (situation 2) with white space
 *
 * Looks like this:
 *  1.   loa[cursor]  => tab hit  =>  loa[d]  => pos:0 currentOrNext:TokenPosType.CURRENT  offsetInToken:3
 *  2.   load [cursor] => tab hit => load [DataSource list]  => pos:0 currentOrNext:TokenPosType.NEXT   offsetInToken:0
 *  2.1  load hi[cursor]ve.`db.table`  => tab hit => load hi[ve]  => pos:1 currentOrNext:TokenPosType.CURRENT  offsetInToken:2
 *
 * the second situation can also show like this:
 *
 * load [cursor]hive.`db.table`
 *
 * we should still show DataSource List. This means we don't care the token after the cursor when we provide
 * suggestion list.
 *
 */
case class TokenPos(pos: Int, currentOrNext: Int, offsetInToken: Int = -1) {
  def str = {
    val posType = currentOrNext match {
      case TokenPosType.NEXT => "next"
      case TokenPosType.CURRENT => "current"
    }
    s"TokenPos: Index(${pos}) currentOrNext(${posType}) offsetInToken(${offsetInToken})"
  }
}


