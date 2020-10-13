package tech.mlsql.ets

import java.util.UUID

import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import streaming.dsl.BranchContext
import tech.mlsql.lang.cmd.compile.internal.gc._

/**
 * 6/10/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class SQLGenContext(session: SparkSession) extends CodegenContext {

  var values: Map[String, Any] = Map()

  private def executeSingleComputeInRuntime(code: String, table: String): Any = {
    val df = session.sql(s"""select ${code} as res from ${table}""")
    //    df.schema.fields.head.dataType match {
    //      case StringType =>
    //    }
    df.collect().head.get(0)
  }

  private def executeAssignComputeInRuntime(code: String, table: String): Unit = {
    val newTable = session.sql(s"""${code} from ${table}""")
    val originTable = session.table(table)
    val (oDataMap, oSchemaMap) = rowToMap(originTable)
    val (nDataMap, nSchemaMap) = rowToMap(newTable)
    val rowMap = oDataMap ++ nDataMap
    val rowSchemaMap = oSchemaMap ++ nSchemaMap
    val newDf = buildBaseTable(rowMap, rowSchemaMap)
    newDf.createOrReplaceTempView(table)
  }

  private def rowToMap(df: DataFrame) = {
    val row = df.collect().head.toSeq
    val fields = df.schema.map(_.name).toSeq
    val schema = df.schema.map(_.dataType).toSeq
    (fields.zip(row).toMap, fields.zip(schema).toMap)
  }

  private def buildBaseTable(_values: Map[String, Any], rowSchemaMap: Map[String, DataType] = Map()) = {
    val fields = _values.keys.toList.map { key =>
      StructField(key, rowSchemaMap.getOrElse(key, StringType))
    }
    val schema = StructType(fields.toSeq)
    val rowItems = fields.map(item => _values(item.name))
    val row = Row.fromSeq(rowItems)
    session.createDataFrame(session.sparkContext.makeRDD(Seq(row)), schema)
  }

  override def execute(exprs: List[Expression], variableTable: VariableTable): Any = {
    val baseTable = buildBaseTable(variableTable.variables.toMap,
      variableTable.types.map(a=>(a._1,a._2.asInstanceOf[DataType])).toMap)
    val uuid = variableTable.name
    baseTable.createOrReplaceTempView(uuid)
    exprs.map { expr =>
      expr match {
        case Select(_) =>
          executeAssignComputeInRuntime(expr.genCode(this).code, uuid)
        case AndAnd(_, _) | OrOr(_, _) =>
          val res = expr.transformUp {
            case AndAnd(left, right) =>
              val leftValue = executeSingleComputeInRuntime(left.genCode(this).code, uuid).asInstanceOf[Boolean]
              val rightValue = executeSingleComputeInRuntime(right.genCode(this).code, uuid).asInstanceOf[Boolean]
              Literal((leftValue && rightValue), Types.Boolean)

            case OrOr(left, right) =>
              val leftValue = executeSingleComputeInRuntime(left.genCode(this).code, uuid).asInstanceOf[Boolean]
              val rightValue = executeSingleComputeInRuntime(right.genCode(this).code, uuid).asInstanceOf[Boolean]
              Literal((leftValue || rightValue), Types.Boolean)
          }
          res

        case _ =>
          val item = executeSingleComputeInRuntime(expr.genCode(this).code, uuid).asInstanceOf[Boolean]
          Literal(item, Types.Boolean)
      }

    }.last
  }
}
