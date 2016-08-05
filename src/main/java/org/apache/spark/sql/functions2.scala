package org.apache.spark.sql

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.ScalaReflection._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{UDTRegistration, UserDefinedType}

import scala.language.implicitConversions
import scala.reflect.runtime.universe.{TypeTag, typeTag}
import scala.util.Try

/**
 * 8/5/16 WilliamZhu(allwefantasy@gmail.com)
 */
object functions2 {
  def udf[RT: TypeTag, A1: TypeTag](className: String, f: Function1[A1, RT]): UserDefinedFunction = {
    val inputTypes = Try(ScalaReflection.schemaFor(typeTag[A1]).dataType :: Nil).toOption
    val udt = UDTRegistration.getUDTFor(className).get.newInstance()
      .asInstanceOf[UserDefinedType[_]]
    UserDefinedFunction(f, Schema(udt, nullable = true).dataType, inputTypes)
  }
}
