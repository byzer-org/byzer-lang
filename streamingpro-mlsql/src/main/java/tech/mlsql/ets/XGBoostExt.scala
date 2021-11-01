package tech.mlsql.ets

import org.apache.spark.ml.param.Param
import streaming.dsl.mmlib._
import tech.mlsql.common.form.{Extra, FormParams, Text}

/**
 * 26/10/2021 WilliamZhu(allwefantasy@gmail.com)
 */
class XGBoostExt extends BaseScriptAlgExt {
  override def code_template: String =
    """
      |include lib.`github.com/allwefantasy/lib-core` where
      |libMirror="gitee.com"
      |and alias="libCore";
      |include local.`libCore.alg.xgboost`;
      |""".stripMargin

  override def etName: String = "__algo_xgboost_operator__"

  override def modelType: ModelType = AlgType

  override def codeExample: Code = Code(SQLCode, "")
  override def doc: Doc = Doc(HtmlDoc,"")

  final val rayAddress: Param[String] = new Param[String](this, "rayAddress",
    FormParams.toJson(Text(
      name = "rayAddress",
      value = "",
      extra = Extra(
        doc =
          """
            | Ray Address. default: 127.0.0.1:10001
          """,
        label = "The ray address",
        options = Map(
          "valueType" -> "string",
          "required" -> "true",
          "derivedType" -> "NONE"
        )), valueProvider = Option(() => {
        ""
      })
    )
    )
  )
  setDefault(rayAddress,"127.0.0.1:10001")
}
