package tech.mlsql.autosuggest.statement

import org.antlr.v4.runtime.Token
import streaming.core.datasource.{DataSourceRegistry, MLSQLSourceInfo}
import streaming.dsl.parser.DSLSQLLexer
import tech.mlsql.autosuggest.{AutoSuggestContext, TableConst, TokenPos, TokenPosType}

import scala.collection.mutable

/**
 * 1/6/2020 WilliamZhu(allwefantasy@gmail.com)
 *
 *
 */
class LoadSuggester(context: AutoSuggestContext, tokens: List[Token], tokenPos: TokenPos) extends StatementSuggester with SuggesterRegister {

  private val subInstances = new mutable.HashMap[String, StatementSuggester]()

  register(classOf[PathSuggester])
  register(classOf[FormatSuggester])
  register(classOf[OptionsSuggester])

  override def register(clzz: Class[_ <: StatementSuggester]): SuggesterRegister = {
    val instance = clzz.getConstructor(classOf[LoadSuggester]).newInstance(this).asInstanceOf[StatementSuggester]
    subInstances.put(instance.name, instance)
    this
  }

  override def isMatch(): Boolean = {
    tokens.headOption.map(_.getType) match {
      case Some(DSLSQLLexer.LOAD) => true
      case _ => false
    }
  }

  override def suggest(): List[SuggestItem] = {
    subInstances.filter(_._2.isMatch()).headOption match {
      case Some(item) => item._2.suggest()
      case None => List()
    }
  }

  class FormatSuggester extends StatementSuggester {
    override def isMatch(): Boolean = {

      (tokenPos.pos, tokenPos.currentOrNext) match {
        case (0, TokenPosType.NEXT) => true
        case (1, TokenPosType.CURRENT) => true
        case (_, _) => false
      }

    }

    override def suggest(): List[SuggestItem] = {
      // datasource type suggest
      val sources = (DataSourceRegistry.allSourceNames.toSet.toSeq ++ Seq(
        "parquet", "csv", "jsonStr", "csvStr", "json", "text", "orc", "kafka", "kafka8", "kafka9", "crawlersql", "image",
        "script", "hive", "xml", "mlsqlAPI", "mlsqlConf"
      )).toList
      LexerUtils.filterPrefixIfNeeded(sources.map(SuggestItem(_, TableConst.DATA_SOURCE_TABLE, Map())), tokens, tokenPos)

    }

    override def name: String = "format"
  }

  class OptionsSuggester extends StatementSuggester {
    override def isMatch(): Boolean = {
      LexerUtils.isInWhereContext(tokens, tokenPos.pos) && LexerUtils.isWhereKey(tokens, tokenPos.pos)
    }

    override def suggest(): List[SuggestItem] = {
      val source = tokens(1)
      val datasources = DataSourceRegistry.fetch(source.getText, Map[String, String]()) match {
        case Some(ds) => ds.asInstanceOf[MLSQLSourceInfo].explainParams(context.session).collect().map(_.getString(0)).toList
        case None => List()
      }
      LexerUtils.filterPrefixIfNeeded(datasources.map(SuggestItem(_, TableConst.OPTION_TABLE, Map())), tokens, tokenPos)

    }

    override def name: String = "options"
  }

  //Here you can implement Hive table / HDFS Path auto suggestion
  class PathSuggester extends StatementSuggester {
    override def isMatch(): Boolean = {
      false
    }

    override def suggest(): List[SuggestItem] = {
      List()
    }

    override def name: String = "path"
  }

  override def name: String = "load"
}


