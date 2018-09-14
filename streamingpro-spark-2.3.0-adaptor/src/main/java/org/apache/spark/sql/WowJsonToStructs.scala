package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.{Expression, JsonToStructs}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DataType

/**
  * Created by allwefantasy on 14/9/2018.
  */
class WowJsonToStructs(
                        schema: DataType,
                        options: Map[String, String],
                        child: Expression,
                        timeZoneId: Option[String],
                        forceNullableSchema: Boolean) extends JsonToStructs(schema, options, child, timeZoneId, forceNullableSchema) {
  def this(schema: DataType, options: Map[String, String], child: Expression) =
    this(schema, options, child, timeZoneId = None,
      forceNullableSchema = SQLConf.get.getConf(SQLConf.FROM_JSON_FORCE_NULLABLE_SCHEMA))
}
