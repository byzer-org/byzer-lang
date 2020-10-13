package tech.mlsql.dsl.adaptor

import streaming.dsl.parser.DSLSQLParser

import scala.collection.mutable.ArrayBuffer

/**
 * 6/10/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class BranchAdaptor(val items: ArrayBuffer[DslAdaptor]) extends DslAdaptor {
  override def parse(ctx: DSLSQLParser.SqlContext): Unit = {

  }
}
