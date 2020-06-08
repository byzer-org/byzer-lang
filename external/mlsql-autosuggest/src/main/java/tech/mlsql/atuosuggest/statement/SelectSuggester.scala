package tech.mlsql.atuosuggest.statement

import org.antlr.v4.runtime.Token
import org.apache.spark.sql.catalyst.parser.SqlBaseLexer
import streaming.dsl.parser.DSLSQLLexer
import tech.mlsql.atuosuggest.dsl.{Food, TokenMatcher, TokenTypeWrapper}
import tech.mlsql.atuosuggest.meta.{MetaTable, MetaTableColumn, MetaTableKey}
import tech.mlsql.atuosuggest.{AutoSuggestContext, TokenPos, TokenPosType}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

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


class ProjectSuggester(selectSuggester: SelectSuggester) extends StatementSuggester with SuggesterRegister {

  val tokens = selectSuggester.tokens
  val tokenPos = selectSuggester.tokenPos
  val fromTableInCurrentScope = ArrayBuffer[MetaTableKeyWrapper]()

  override def name: String = "project"

  override def isMatch(): Boolean = {
    //make sure the pos  after select and [before from or is the end of the statement]
    // it's ok if we are wrong.
    tokens.zipWithIndex.filter(_._1.getType == SqlBaseLexer.FROM).headOption match {
      case Some(from) => from._2 > tokenPos.pos
      case None => true
    }

  }

  def level = {
    var targetLevel = 0
    selectSuggester.sqlAST.visitDown(0) { case (ast, _level) =>
      if (tokenPos.pos >= ast.start && tokenPos.pos < ast.stop) targetLevel = _level
    }
    targetLevel
  }

  def table_info = {
    val _level = if (selectSuggester.table_info.size == 1 && level == 0) 0 else level + 1
    selectSuggester.table_info.get(_level)
  }


  private def tableSuggest(): List[SuggestItem] = {
    table_info match {
      case Some(tb) => tb.keySet.map { key =>
        key.aliasName.getOrElse(key.metaTableKey.table)
      }.map(SuggestItem(_)).toList
      case None => List()
    }
  }

  private def attributeSuggest(): List[SuggestItem] = {
    val tempStart = tokenPos.currentOrNext match {
      case TokenPosType.CURRENT =>
        tokenPos.pos - 1
      case TokenPosType.NEXT =>
        tokenPos.pos
    }

    def allOutput = {
      table_info.get.flatMap { case (_, metaTable) =>
        metaTable.columns.map(column => SuggestItem(column.name)).toList
      }.toList
    }

    val temp = TokenMatcher(tokens, tempStart).back.eat(Food(None, TokenTypeWrapper.DOT), Food(None, SqlBaseLexer.IDENTIFIER)).build
    if (temp.isSuccess) {
      val table = temp.getMatchTokens.head.getText
      table_info.get.filter { case (key, value) =>
        key.aliasName.isDefined && key.aliasName.get == table
      }.headOption match {
        case Some(table) => table._2.columns.map(column => SuggestItem(column.name)).toList
        case None => allOutput
      }
    } else allOutput

  }

  private def functionSuggest(): List[SuggestItem] = {
    List()
  }

  override def suggest(): List[SuggestItem] = {
    LexerUtils.filterPrefixIfNeeded(tableSuggest() ++ attributeSuggest() ++ functionSuggest(), tokens, tokenPos)
  }

  override def register(clzz: Class[_ <: StatementSuggester]): SuggesterRegister = ???
}

class FromSuggester

class GroupSuggester

class HavingSuggester

class SubQuerySuggester

class JoinSuggester

class FunctionSuggester



