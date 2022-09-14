package tech.mlsql.autosuggest.app


/**
 * 2019-07-18 WilliamZhu(allwefantasy@gmail.com)
 */
object RawDBTypeToJavaType {
  def convert(dbType: String, name: String) = {
    dbType match {
      case "mysql" => MysqlType.valueOf(name.toUpperCase()).getJdbcType
      case _ => throw new RuntimeException(s"dbType ${dbType} is not supported yet")
    }

  }

}
