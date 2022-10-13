package org.apache.spark.sql.jdbc

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import tech.mlsql.common.utils.log.Logging

class UpsertBuilderTest extends AnyFunSuite with Logging {
  val idField = Seq(StructField("c1", StringType, nullable = false))
  val schema = StructType(Seq(StructField("c1", StringType, nullable = false),
    StructField("c2", IntegerType, nullable = true)))

  test("generating oracle merge into statement and schema") {
    val (stmt, upsertSchema) = OracleUpsertBuilder.generateStatement("test_table", idField, schema)
    if (log.isInfoEnabled()) {
      log.info(stmt)
    }
    assert(upsertSchema.fields.length == 4, "There should be 4 fields in schema")
    assert(stmt.startsWith("MERGE INTO test_table"))
  }

  test("generating mysql insert on duplicate statement and schema") {
    val dialect = JdbcDialects.get("jdbc:mysql://127.0.0.1:3306")
    val (stmt, upsertSchema) = MysqlUpsertBuilder.generateStatement("table_1", dialect, idField, schema)
    if (log.isInfoEnabled()) {
      log.info(stmt)
    }
    assert(stmt.startsWith("insert into table_1"))
    assert(upsertSchema.length == 3 )
  }
}
