package tech.mlsql.autosuggest

import com.intigua.antlr4.autosuggest.LexerWrapper
import org.antlr.v4.runtime.misc.Interval
import org.antlr.v4.runtime.{CharStream, CodePointCharStream, IntStream, Token}
import org.apache.spark.sql.SparkSession
import streaming.dsl.parser.DSLSQLLexer
import tech.mlsql.autosuggest.meta.{LoadTableProvider, MetaProvider, MetaTable, MetaTableKey}
import tech.mlsql.autosuggest.statement.{LoadSuggester, SelectSuggester, SuggestItem}
import tech.mlsql.common.utils.reflect.ClassPath

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object AutoSuggestContext {
  var isInit = false

  def init: Unit = {
    val funcRegs = ClassPath.from(classOf[AutoSuggestContext].getClassLoader).getTopLevelClasses("tech.mlsql.autosuggest.funcs").iterator()
    while (funcRegs.hasNext) {
      val wow = funcRegs.next()
      val funcMetaTable = wow.load().newInstance().asInstanceOf[FuncReg].register
      MLSQLSQLFunction.funcMetaProvider.register(funcMetaTable)
    }
    isInit = true
  }

  if (!isInit) {
    init
  }

}

/**
 * 2/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class AutoSuggestContext(val session: SparkSession,
                         val lexer: LexerWrapper,
                         val rawSQLLexer: LexerWrapper) {


  val statements = ArrayBuffer[List[Token]]()
  val loadTableProvider: LoadTableProvider = new LoadTableProvider()
  var metaProvider: MetaProvider = new MetaProvider {
    override def search(key: MetaTableKey): Option[MetaTable] = None

    override def list: List[MetaTable] = List()
  }
  val TEMP_TABLES_IN_SCRIPT = new mutable.HashMap[MetaTableKey, MetaTable]()
  val TEMP_TABLES_IN_CURRENT_SQL = new mutable.HashMap[MetaTableKey, MetaTable]()

  def setMetaProvider(_metaProvider: MetaProvider) = {
    metaProvider = _metaProvider
  }

  def build(_tokens: List[Token]): AutoSuggestContext = {
    val tokens = _tokens.zipWithIndex
    var start = 0
    var end = 0
    tokens.foreach { case (token, index) =>
      // statement end
      if (token.getType == DSLSQLLexer.T__1) {
        end = index
        statements.append(tokens.filter(p => p._2 >= start && p._2 <= end).map(_._1))
        start = index + 1
      }

    }
    // clean the last statement without ender
    val theLeft = tokens.filter(p => p._2 >= start && p._2 <= tokens.size).map(_._1).toList
    if (theLeft.size > 0) {
      statements.append(theLeft)
    }
    return this
  }

  def toRelativePos(tokenPos: TokenPos): (TokenPos, Int) = {
    var skipSize = 0
    var targetIndex = 0
    var targetPos: TokenPos = null
    var targetStaIndex = 0
    statements.zipWithIndex.foreach { case (sta, index) =>
      val relativePos = tokenPos.pos - skipSize
      if (relativePos >= 0 && relativePos < sta.size) {
        targetPos = tokenPos.copy(pos = tokenPos.pos - skipSize)
        targetStaIndex = index
      }
      skipSize += sta.size
      targetIndex += 1
    }
    return (targetPos, targetStaIndex)
  }

  /**
   * Notice that the pos in tokenPos is in whole script.
   * We need to convert it to the relative pos in every statement
   */
  def suggest(tokenPos: TokenPos) = {

    val (relativeTokenPos, index) = toRelativePos(tokenPos)
    val items = statements(index).headOption.map(_.getText) match {
      case Some("load") =>
        val suggester = new LoadSuggester(this, statements(index), relativeTokenPos)
        suggester.suggest()
      case Some("select") =>
        val suggester = new SelectSuggester(this, statements(index), relativeTokenPos)
        suggester.suggest()
      case Some(value) => firstWords.filter(_.name.startsWith(value))
      case None => firstWords
    }
    items.distinct
  }

  private val firstWords = List("load", "select", "include", "register", "run", "train", "save", "set").map(SuggestItem(_, TableConst.KEY_WORD_TABLE, Map())).toList


}

class UpperCaseCharStream(wrapped: CodePointCharStream) extends CharStream {
  override def consume(): Unit = wrapped.consume

  override def getSourceName(): String = wrapped.getSourceName

  override def index(): Int = wrapped.index

  override def mark(): Int = wrapped.mark

  override def release(marker: Int): Unit = wrapped.release(marker)

  override def seek(where: Int): Unit = wrapped.seek(where)

  override def size(): Int = wrapped.size

  override def getText(interval: Interval): String = {
    // ANTLR 4.7's CodePointCharStream implementations have bugs when
    // getText() is called with an empty stream, or intervals where
    // the start > end. See
    // https://github.com/antlr/antlr4/commit/ac9f7530 for one fix
    // that is not yet in a released ANTLR artifact.
    if (size() > 0 && (interval.b - interval.a >= 0)) {
      wrapped.getText(interval)
    } else {
      ""
    }
  }

  override def LA(i: Int): Int = {
    val la = wrapped.LA(i)
    if (la == 0 || la == IntStream.EOF) la
    else Character.toUpperCase(la)
  }
}
