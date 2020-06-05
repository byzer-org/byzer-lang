package tech.mlsql.atuosuggest.statement

import org.antlr.v4.runtime.Token
import org.apache.spark.sql.catalyst.parser.SqlBaseLexer
import streaming.dsl.parser.DSLSQLLexer
import tech.mlsql.atuosuggest.meta.{MetaTable, MetaTableColumn, MetaTableKey}
import tech.mlsql.atuosuggest.{AutoSuggestContext, TokenPos}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * 3/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class SelectSuggester(val context: AutoSuggestContext, val _tokens: List[Token], val tokenPos: TokenPos) extends StatementSuggester with SuggesterRegister {

  private val subInstances = new mutable.HashMap[String, StatementSuggester]()

  override def name: String = "select"

  private lazy val newTokens = LexerUtils.toRawSQLTokens(context, _tokens)
  private lazy val TABLE_INFO = mutable.HashMap[Int, mutable.HashMap[MetaTableKeyWrapper, MetaTable]]()
  private lazy val selectTree: SingleStatementAST = buildTree()

  def sqlAST = selectTree

  def tokens = newTokens

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
    selectTree.children.size
    TABLE_INFO.foreach { case (level, info) =>
      println(s"${level} =>")
      println(info)
    }

    List()

  }

  override def register(clzz: Class[_ <: StatementSuggester]): SuggesterRegister = {
    val instance = clzz.getConstructor(classOf[SelectSuggester]).newInstance(this).asInstanceOf[StatementSuggester]
    subInstances.put(instance.name, instance)
    this
  }
}


class ProjectSuggester(selectSuggester: SelectSuggester) extends StatementSuggester with SuggesterRegister {

  val tokens = selectSuggester._tokens
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

  override def suggest(): List[SuggestItem] = {
    // try to get all information from all statement.
    ???
  }

  override def register(clzz: Class[_ <: StatementSuggester]): SuggesterRegister = ???
}

class FromSuggester

class GroupSuggester

class HavingSuggester

class SubQuerySuggester

class JoinSuggester

class FunctionSuggester



