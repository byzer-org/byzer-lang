package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.types.DataType

case class WowScalaUDF(function: AnyRef,
                       dataType: DataType,
                       children: Seq[Expression],
                       inputsNullSafe: Seq[Boolean],
                       inputTypes: Seq[DataType] = Nil,
                       udfName: Option[String] = None,
                       nullable: Boolean = true,
                       udfDeterministic: Boolean = true) {

  def this(
            function: AnyRef,
            dataType: DataType,
            children: Seq[Expression]
          ) = {
    this(
      function, dataType, children, Nil,
      Nil, None, nullable = true, udfDeterministic = true)
  }


  def toScalaUDF = {
    new ScalaUDF(function,
      dataType,
      children,
      inputTypes,
      udfName,
      nullable,
      udfDeterministic
    )
  }

}
