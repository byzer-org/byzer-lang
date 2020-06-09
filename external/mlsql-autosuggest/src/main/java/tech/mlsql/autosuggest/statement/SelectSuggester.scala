package tech.mlsql.autosuggest.statement

import org.antlr.v4.runtime.Token
import org.apache.spark.sql.catalyst.parser.SqlBaseLexer
import streaming.dsl.parser.DSLSQLLexer
import tech.mlsql.autosuggest.dsl.{Food, TokenMatcher}
import tech.mlsql.autosuggest.meta.{MetaTable, MetaTableColumn, MetaTableKey}
import tech.mlsql.autosuggest.{AutoSuggestContext, TokenPos}

import scala.collection.mutable

/**
 * 3/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class SelectSuggester(val context: AutoSuggestContext, val _tokens: List[Token], val tokenPos: TokenPos) extends StatementSuggester with SuggesterRegister {

  private val subInstances = new mutable.HashMap[String, StatementSuggester]()
  register(classOf[ProjectSuggester])
  register(classOf[FromSuggester])
  register(classOf[FilterSuggester])
  register(classOf[JoinSuggester])
  register(classOf[JoinOnSuggester])
  register(classOf[OrderSuggester])

  override def name: String = "select"

  private lazy val newTokens = LexerUtils.toRawSQLTokens(context, _tokens)
  private lazy val TABLE_INFO = mutable.HashMap[Int, mutable.HashMap[MetaTableKeyWrapper, MetaTable]]()
  private lazy val selectTree: SingleStatementAST = buildTree()

  def sqlAST = selectTree

  def tokens = newTokens

  def table_info = TABLE_INFO

  override def isMatch(): Boolean = {
    _tokens.headOption.map(_.getType) match {
      case Some(DSLSQLLexer.SELECT) => true
      case _ => false
    }
  }

  private def buildTree() = {
    val root = SingleStatementAST.build(this, newTokens)
    import scala.collection.mutable

    root.visitUp(level = 0) { case (ast: SingleStatementAST, level: Int) =>
      if (!TABLE_INFO.contains(level)) {
        TABLE_INFO.put(level, new mutable.HashMap[MetaTableKeyWrapper, MetaTable]())
      }
      if (ast.isLeaf) {
        ast.tables(newTokens).map { item =>
          context.metaProvider.search(item.metaTableKey) match {
            case Some(res) =>
              TABLE_INFO(level) += (item -> res)
            case None =>
          }
        }
      }
      val nameOpt = ast.name(newTokens)
      if (nameOpt.isDefined) {

        val metaTableKey = MetaTableKey(None, None, null)
        val metaTableKeyWrapper = MetaTableKeyWrapper(metaTableKey, nameOpt)
        val metaColumns = ast.output(newTokens).map { attr =>
          MetaTableColumn(attr, null, true, Map())
        }
        TABLE_INFO(level) += (metaTableKeyWrapper -> MetaTable(metaTableKey, metaColumns))

      }

    }

    root
  }

  override def suggest(): List[SuggestItem] = {
    var instance: StatementSuggester = null
    subInstances.foreach { _instance =>
      if (instance == null && _instance._2.isMatch()) {
        instance = _instance._2
      }
    }
    if (instance == null) List()
    else instance.suggest()

  }

  override def register(clzz: Class[_ <: StatementSuggester]): SuggesterRegister = {
    val instance = clzz.getConstructor(classOf[SelectSuggester]).newInstance(this).asInstanceOf[StatementSuggester]
    subInstances.put(instance.name, instance)
    this
  }
}


class ProjectSuggester(_selectSuggester: SelectSuggester) extends StatementSuggester with StatementUtils with SuggesterRegister {

  def tokens = _selectSuggester.tokens

  def tokenPos = _selectSuggester.tokenPos

  def selectSuggester = _selectSuggester

  def backAndFirstIs(t: Int, keywords: List[Int] = TokenMatcher.SQL_SPLITTER_KEY_WORDS): Boolean = {
    // 能找得到所在的子查询（也可以是最外层）
    val ast = getASTFromTokenPos
    if (ast.isEmpty) return false

    // 从光标位置去找第一个核心词
    val temp = TokenMatcher(tokens, tokenPos.pos).back.orIndex(keywords.map(Food(None, _)).toArray)
    if (temp == -1) return false
    //第一个核心词必须是是定的词，并且在子查询里
    if (tokens(temp).getType == t && temp >= ast.get.start && temp < ast.get.stop) return true
    return false
  }


  override def name: String = "project"

  override def isMatch(): Boolean = {
    backAndFirstIs(SqlBaseLexer.SELECT)

  }

  override def suggest(): List[SuggestItem] = {
    LexerUtils.filterPrefixIfNeeded(tableSuggest() ++ attributeSuggest() ++ functionSuggest(), tokens, tokenPos)
  }

  override def register(clzz: Class[_ <: StatementSuggester]): SuggesterRegister = ???
}

class FilterSuggester(_selectSuggester: SelectSuggester) extends ProjectSuggester(_selectSuggester) {


  override def name: String = "filter"

  override def isMatch(): Boolean = {
    backAndFirstIs(SqlBaseLexer.WHERE)

  }

  override def suggest(): List[SuggestItem] = {
    LexerUtils.filterPrefixIfNeeded(tableSuggest() ++ attributeSuggest() ++ functionSuggest(), tokens, tokenPos)
  }

  override def register(clzz: Class[_ <: StatementSuggester]): SuggesterRegister = ???
}

class JoinOnSuggester(_selectSuggester: SelectSuggester) extends ProjectSuggester(_selectSuggester) {
  override def name: String = "join_on"

  override def isMatch(): Boolean = {
    backAndFirstIs(SqlBaseLexer.ON)
  }

  override def suggest(): List[SuggestItem] = {
    LexerUtils.filterPrefixIfNeeded(tableSuggest() ++ attributeSuggest() ++ functionSuggest(), tokens, tokenPos)
  }
}

class JoinSuggester(_selectSuggester: SelectSuggester) extends ProjectSuggester(_selectSuggester) {
  override def name: String = "join"

  override def isMatch(): Boolean = {
    backAndFirstIs(SqlBaseLexer.JOIN)
  }

  override def suggest(): List[SuggestItem] = {
    LexerUtils.filterPrefixIfNeeded(tableSuggest(), tokens, tokenPos)
  }
}

class FromSuggester(_selectSuggester: SelectSuggester) extends ProjectSuggester(_selectSuggester) {
  override def name: String = "from"

  override def isMatch(): Boolean = {
    backAndFirstIs(SqlBaseLexer.FROM)
  }

  override def suggest(): List[SuggestItem] = {
    LexerUtils.filterPrefixIfNeeded(tableSuggest(), tokens, tokenPos)
  }
}

class OrderSuggester(_selectSuggester: SelectSuggester) extends ProjectSuggester(_selectSuggester) {
  override def name: String = "order"

  override def isMatch(): Boolean = {
    backAndFirstIs(SqlBaseLexer.FROM)
  }

  override def suggest(): List[SuggestItem] = {
    LexerUtils.filterPrefixIfNeeded(attributeSuggest() ++ functionSuggest(), tokens, tokenPos)
  }
}










