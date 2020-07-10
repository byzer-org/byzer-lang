package tech.mlsql.autosuggest

import com.intigua.antlr4.autosuggest.LexerWrapper
import org.antlr.v4.runtime.misc.Interval
import org.antlr.v4.runtime.{CharStream, CodePointCharStream, IntStream, Token}
import org.apache.spark.sql.SparkSession
import tech.mlsql.autosuggest.meta._
import tech.mlsql.autosuggest.preprocess.TablePreprocessor
import tech.mlsql.autosuggest.statement._
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.common.utils.reflect.ClassPath

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

object AutoSuggestContext {

  val memoryMetaProvider = new MemoryMetaProvider()
  var isInit = false

  def init: Unit = {
    val funcRegs = ClassPath.from(classOf[AutoSuggestContext].getClassLoader).getTopLevelClasses(POrCLiterals.FUNCTION_PACKAGE).iterator()
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
 * 每个请求都需要实例化一个
 */
class AutoSuggestContext(val session: SparkSession,
                         val lexer: LexerWrapper,
                         val rawSQLLexer: LexerWrapper) extends Logging {

  private var _debugMode = false

  private var _rawTokens: List[Token] = List()
  private var _statements = List[List[Token]]()
  private val _tempTableProvider: StatementTempTableProvider = new StatementTempTableProvider()
  private var _rawLineNum = 0
  private var _rawColumnNum = 0
  private var userDefinedProvider: MetaProvider = new MetaProvider {
    override def search(key: MetaTableKey, extra: Map[String, String] = Map()): Option[MetaTable] = None

    override def list(extra: Map[String, String] = Map()): List[MetaTable] = List()
  }
  private var _metaProvider: MetaProvider = new LayeredMetaProvider(tempTableProvider, userDefinedProvider)

  private val _statementProcessors = ArrayBuffer[PreProcessStatement]()
  addStatementProcessor(new TablePreprocessor(this))

  private var _statementSplitter: StatementSplitter = new MLSQLStatementSplitter()

  def setDebugMode(isDebug: Boolean) = {
    this._debugMode = isDebug
  }

  def isInDebugMode = _debugMode

  def metaProvider = _metaProvider

  def statements = {
    _statements
  }

  def rawTokens = _rawTokens

  def tempTableProvider = {
    _tempTableProvider
  }

  def setStatementSplitter(_statementSplitter: StatementSplitter) = {
    this._statementSplitter = _statementSplitter
    this
  }

  def setUserDefinedMetaProvider(_metaProvider: MetaProvider) = {
    userDefinedProvider = _metaProvider
    this._metaProvider = new LayeredMetaProvider(tempTableProvider, userDefinedProvider)
    this
  }

  def setRootMetaProvider(_metaProvider: MetaProvider) = {
    this._metaProvider = _metaProvider
    this
  }

  def addStatementProcessor(item: PreProcessStatement) = {
    this._statementProcessors += item
    this
  }

  def buildFromString(str: String): AutoSuggestContext = {
    build(lexer.tokenizeNonDefaultChannel(str).tokens.asScala.toList)
    this
  }

  def build(_tokens: List[Token]): AutoSuggestContext = {
    _rawTokens = _tokens
    _statements = _statementSplitter.split(rawTokens)
    // preprocess
    _statementProcessors.foreach { sta =>
      _statements.foreach(sta.process(_))
    }
    return this
  }

  def toRelativePos(tokenPos: TokenPos): (TokenPos, Int) = {
    var skipSize = 0
    var targetIndex = 0
    var targetPos: TokenPos = null
    var targetStaIndex = 0
    _statements.zipWithIndex.foreach { case (sta, index) =>
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

  def suggest(lineNum: Int, columnNum: Int): List[SuggestItem] = {
    if (isInDebugMode) {
      logInfo(s"lineNum:${lineNum} columnNum:${columnNum}")
    }
    _rawLineNum = lineNum
    _rawColumnNum = columnNum
    val tokenPos = LexerUtils.toTokenPos(rawTokens, lineNum, columnNum)
    _suggest(tokenPos)
  }

  /**
   * Notice that the pos in tokenPos is in whole script.
   * We need to convert it to the relative pos in every statement
   */
  private[autosuggest] def _suggest(tokenPos: TokenPos): List[SuggestItem] = {
    assert(_rawColumnNum != 0 || _rawColumnNum != 0, "lineNum and columnNum should be set")
    if (isInDebugMode) {
      logInfo("Global Pos::" + tokenPos.str + s"::${rawTokens(tokenPos.pos)}")
    }
    if (tokenPos.pos == -1) {
      return firstWords
    }
    val (relativeTokenPos, index) = toRelativePos(tokenPos)

    if (isInDebugMode) {
      logInfo(s"Relative Pos in ${index}-statement ::" + relativeTokenPos.str)
      logInfo(s"${index}-statement:\n${_statements(index).map(_.getText).mkString(" ")}")
    }
    val items = _statements(index).headOption.map(_.getText) match {
      case Some("load") =>
        val suggester = new LoadSuggester(this, _statements(index), relativeTokenPos)
        suggester.suggest()
      case Some("select") =>
        // we should recompute the token pos since they use spark sql lexer instead of 
        val selectTokens = _statements(index)
        val startLineNum = selectTokens.head.getLine
        val relativeLineNum = _rawLineNum - startLineNum + 1 // startLineNum start from 1
        val relativeColumnNum = _rawColumnNum - selectTokens.head.getCharPositionInLine // charPos is start from 0

        if (isInDebugMode) {
          logInfo(s"select[${index}] relativeLineNum:${relativeLineNum} relativeColumnNum:${relativeColumnNum}")
        }
        val relativeTokenPos = LexerUtils.toTokenPosForSparkSQL(LexerUtils.toRawSQLTokens(this, selectTokens), relativeLineNum, relativeColumnNum)
        if (isInDebugMode) {
          logInfo(s"select[${index}] relativeTokenPos:${relativeTokenPos.str}")
        }
        val suggester = new SelectSuggester(this, selectTokens, relativeTokenPos)
        suggester.suggest()
      case Some(value) => firstWords.filter(_.name.startsWith(value))
      case None => firstWords
    }
    items.distinct
  }

  private val firstWords = List("load", "select", "include", "register", "run", "train", "save", "set").map(SuggestItem(_, SpecialTableConst.KEY_WORD_TABLE, Map())).toList
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
