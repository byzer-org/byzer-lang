package tech.mlsql.atuosuggest.statement

import org.antlr.v4.runtime.Token
import org.apache.spark.sql.catalyst.parser.SqlBaseLexer
import streaming.dsl.parser.DSLSQLLexer
import tech.mlsql.atuosuggest.{AutoSuggestContext, TokenPos}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * 3/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class SelectSuggester(val context: AutoSuggestContext, val tokens: List[Token], val tokenPos: TokenPos) extends StatementSuggester with SuggesterRegister {

  private val subInstances = new mutable.HashMap[String, StatementSuggester]()

  override def name: String = "select"

  override def isMatch(): Boolean = {
    tokens.headOption.map(_.getType) match {
      case Some(DSLSQLLexer.SELECT) => true
      case _ => false
    }
  }

  override def suggest(): List[SuggestItem] = {
    val newTokens = LexerUtils.toRawSQLTokens(context, tokens)
    val root = SingleStatementAST.build(this, newTokens)
    
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



