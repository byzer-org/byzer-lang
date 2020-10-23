package tech.mlsql.nativelib.runtime

import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, String2StringExpression, UnaryExpression}
import org.apache.spark.unsafe.types.UTF8String

/**
 * 20/10/2020 WilliamZhu(allwefantasy@gmail.com)
 */
case class NativeLower(child: Expression) extends UnaryExpression with String2StringExpression {

  override def convert(v: UTF8String): UTF8String = {
    v.toLowerCase
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, c => s"UTF8String.fromString(tech.mlsql.nativelib.runtime.MLSQLNativeRuntime.funcLower(($c).toString()))")
  }
}
