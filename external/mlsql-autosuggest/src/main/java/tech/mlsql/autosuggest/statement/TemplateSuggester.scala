package tech.mlsql.autosuggest.statement

import org.antlr.v4.runtime.Token
import streaming.dsl.parser.DSLSQLLexer
import tech.mlsql.autosuggest.{AutoSuggestContext, TokenPos}

import scala.collection.mutable

/**
 * 30/6/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class TemplateSuggester(val context: AutoSuggestContext, val _tokens: List[Token], val _tokenPos: TokenPos) extends StatementSuggester
  with SuggesterRegister {
  private val subInstances = new mutable.HashMap[String, StatementSuggester]()


  override def register(clzz: Class[_ <: StatementSuggester]): SuggesterRegister = {
    val instance = clzz.getConstructor(classOf[LoadSuggester]).newInstance(this).asInstanceOf[StatementSuggester]
    subInstances.put(instance.name, instance)
    this
  }

  override def isMatch(): Boolean = {
    _tokens.headOption.map(_.getType) match {
      case Some(DSLSQLLexer.REGISTER) => true
      case _ => false
    }
  }


  override def suggest(): List[SuggestItem] = {
    List()
  }


  override def name: String = "template"
}

