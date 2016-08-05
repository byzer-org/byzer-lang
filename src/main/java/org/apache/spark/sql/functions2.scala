package org.apache.spark.sql

import _root_.streaming.common.SparkCompatibility
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.ScalaReflection._
import org.apache.spark.sql.types.{DataType, SQLUserDefinedType, UserDefinedType}

import scala.language.implicitConversions
import scala.reflect.runtime.universe.{TypeTag, typeTag}
import scala.util.Try

/**
 * 8/5/16 WilliamZhu(allwefantasy@gmail.com)
 */
object functions2 {
  //org.apache.spark.sql.expressions.UserDefinedFunction
  def udf[RT: TypeTag, A1: TypeTag](className: String, f: Function1[A1, RT]): Any = {

    if (SparkCompatibility.sparkVersion.startsWith("2")) {
      val inputTypes = Try(ScalaReflection.schemaFor(typeTag[A1]).dataType :: Nil).toOption
      val dufReg = Class.forName("org.apache.spark.sql.types.UDTRegistration").
        getMethod("getUDTFor", classOf[String]).invoke(null, className).asInstanceOf[Option[Class[_]]]
      val udt = dufReg.get.newInstance().asInstanceOf[UserDefinedType[_]]
      Class.forName("org.apache.spark.sql.expressions.UserDefinedFunction").
        getConstructor(classOf[AnyRef], classOf[DataType], classOf[Option[Seq[DataType]]]).
        newInstance(f, Schema(udt, nullable = true).dataType, inputTypes)
    } else {
      val inputTypes = Try(ScalaReflection.schemaFor(typeTag[A1]).dataType :: Nil).toOption.get.toSeq
      val udt = org.apache.spark.util.Utils.classForName(className)
        .getAnnotation(classOf[SQLUserDefinedType]).udt().newInstance().asInstanceOf[UserDefinedType[_]]
      Class.forName("org.apache.spark.sql.UserDefinedFunction").
        getConstructor(classOf[AnyRef], classOf[DataType], classOf[Seq[DataType]]).
        newInstance(f, Schema(udt, nullable = true).dataType, inputTypes)

    }


  }
}
