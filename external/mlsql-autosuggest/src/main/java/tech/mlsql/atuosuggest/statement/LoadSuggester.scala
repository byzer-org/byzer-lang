package tech.mlsql.atuosuggest.statement

import org.antlr.v4.runtime.Token
import streaming.core.datasource.{DataSourceRegistry, MLSQLSourceInfo}
import streaming.dsl.parser.DSLSQLLexer
import tech.mlsql.atuosuggest.{AutoSuggestContext, TokenPos, TokenPosType}

/**
 * 1/6/2020 WilliamZhu(allwefantasy@gmail.com)
 *
 *
 */
class LoadSuggester(context: AutoSuggestContext, tokens: List[Token], tokenPos: TokenPos) extends StatementSuggester {
  val formatSuggester = new FormatSuggester(context, tokens, tokenPos)
  val optionsSuggester = new OptionsSuggester(context, tokens, tokenPos)

  override def isMatch(): Boolean = {
    tokens.headOption.map(_.getType) match {
      case Some(DSLSQLLexer.LOAD) => true
      case _ => false
    }
  }

  override def suggest(): List[String] = {
    if (formatSuggester.isMatch()) return formatSuggester.suggest()
    if (optionsSuggester.isMatch()) return optionsSuggester.suggest()
    List()
  }

  class FormatSuggester(context: AutoSuggestContext, tokens: List[Token], tokenPos: TokenPos) extends StatementSuggester {
    override def isMatch(): Boolean = {

      (tokenPos.pos, tokenPos.currentOrNext) match {
        case (0, TokenPosType.NEXT) => true
        case (1, TokenPosType.CURRENT) => true
        case (_, _) => false
      }

    }

    override def suggest(): List[String] = {
      // datasource type suggest
      val sources = (DataSourceRegistry.allSourceNames.toSet.toSeq ++ Seq(
        "parquet", "csv", "jsonStr", "csvStr", "json", "text", "orc", "kafka", "kafka8", "kafka9", "crawlersql", "image",
        "script", "hive", "xml", "mlsqlAPI", "mlsqlConf"
      )).toList
      if (tokenPos.offsetInToken != 0) {
        return sources.filter(s => s.startsWith(tokens(tokenPos.pos).getText.substring(0, tokenPos.offsetInToken)))
      }
      return sources

    }
  }

  class OptionsSuggester(context: AutoSuggestContext, tokens: List[Token], tokenPos: TokenPos) extends StatementSuggester {
    override def isMatch(): Boolean = {
      LexerUtils.isInWhereContext(tokens, tokenPos.pos) && LexerUtils.isWhereKey(tokens, tokenPos.pos)
    }

    override def suggest(): List[String] = {
      val source = tokens(1)
      DataSourceRegistry.fetch(source.getText, Map[String, String]()) match {
        case Some(ds) => ds.asInstanceOf[MLSQLSourceInfo].explainParams(context.session).collect().map(_.getString(0)).toList
        case None => List()
      }
    }
  }

  //Here you can implement Hive table / HDFS Path auto suggestion
  class PathSuggester extends StatementSuggester {
    override def isMatch(): Boolean = {
      false
    }

    override def suggest(): List[String] = {
      List()
    }
  }

}


