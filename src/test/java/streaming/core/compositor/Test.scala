package streaming.core.compositor


/**
  * Created by allwefantasy on 8/3/2017.
  */
object Test {
  val doc = Map[Any, Any]()

  import net.sf.json.JSONObject

  val obj = JSONObject.fromObject(doc("bbs-web-influence").toString)
  val influence_score = obj.getLong("influence_score")
  val actionName = obj.getString("influence_score")
  val rowkey = doc("rowkey").toString
  Map(
    "influence_score" -> influence_score,
    "rowkey" -> rowkey,
    "actionName" -> actionName
  )
}
