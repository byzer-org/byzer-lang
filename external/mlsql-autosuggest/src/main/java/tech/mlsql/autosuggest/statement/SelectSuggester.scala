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


  override def name: String = "project"

  override def isMatch(): Boolean = {
    /**
     *  0. 找到子句（自下而上遍历）
     *  1. 在子句里找到from所在的位置，对比是不是在from前面
     *  2. 如果没有找到from （可能用户还没来得及写from）,则认为就是在select语句里
     */
    val ast = getASTFromTokenPos
    if (ast.isEmpty) return false
    val fromPos = TokenMatcher(tokens, ast.get.start).index(Array(Food(None, SqlBaseLexer.FROM)))
    if (fromPos == -1) return true
    fromPos < ast.get.stop && tokenPos.pos < fromPos

  }

  override def suggest(): List[SuggestItem] = {
    LexerUtils.filterPrefixIfNeeded(tableSuggest() ++ attributeSuggest() ++ functionSuggest(), tokens, tokenPos)
  }

  override def register(clzz: Class[_ <: StatementSuggester]): SuggesterRegister = ???
}





