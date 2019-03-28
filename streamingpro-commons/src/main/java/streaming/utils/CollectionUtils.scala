package streaming.utils

object CollectionUtils {
  def list2Map(fieldNames: Array[String]):Map[String, Int] = {
    var index = -1
    fieldNames.map(field => {
      index = index + 1
      field -> index
    }).toMap
  }
}
