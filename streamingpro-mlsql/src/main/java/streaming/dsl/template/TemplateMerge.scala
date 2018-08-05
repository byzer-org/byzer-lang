package streaming.dsl.template

import org.joda.time.DateTime
import streaming.common.RenderEngine

/**
  * Created by allwefantasy on 29/3/2018.
  */
object TemplateMerge {

  def merge(sql: String, root: Map[String, String]) = {

    val dformat = "yyyy-MM-dd"
    //2018-03-24
    val predified_variables = Map[String, String](
      "yesterday" -> DateTime.now().minusDays(1).toString(dformat),
      "today" -> DateTime.now().toString(dformat),
      "tomorrow" -> DateTime.now().plusDays(1).toString(dformat),
      "theDayBeforeYesterday" -> DateTime.now().minusDays(2).toString(dformat)
    )
    val newRoot = predified_variables ++ root

    RenderEngine.render(sql, newRoot)
  }
}
