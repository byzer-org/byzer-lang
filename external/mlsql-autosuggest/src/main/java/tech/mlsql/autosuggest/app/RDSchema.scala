package tech.mlsql.autosuggest.app

import java.sql.{JDBCType, SQLException}

import com.alibaba.druid.sql.SQLUtils
import com.alibaba.druid.sql.ast.SQLDataType
import com.alibaba.druid.sql.ast.statement.{SQLColumnDefinition, SQLCreateTableStatement}
import com.alibaba.druid.sql.repository.SchemaRepository
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types.DecimalType.{MAX_PRECISION, MAX_SCALE}
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._
import scala.math.min

/**
  * 2019-07-18 WilliamZhu(allwefantasy@gmail.com)
  */
class RDSchema(dbType: String) {

  private val repository = new SchemaRepository(dbType)

  def createTable(sql: String) = {
    repository.console(sql)
    val tableName = SQLUtils.parseStatements(sql, dbType).get(0).asInstanceOf[SQLCreateTableStatement].
      getTableSource.getName.getSimpleName
    SQLUtils.normalize(tableName)
  }

  def getTableSchema(table: String) = {
    val dialect = JdbcDialects.get(s"jdbc:${dbType}")


    def extractfieldSize = (dataType: SQLDataType) => {
      dataType.getArguments.asScala.map { f =>
        try {
          f.toString.toInt
        } catch {
          case e: Exception => 0
        }

      }.headOption
    }

    val fields = repository.findTable(table).getStatement.asInstanceOf[SQLCreateTableStatement].
      getTableElementList().asScala.filter(f => f.isInstanceOf[SQLColumnDefinition]).
      map {
        _.asInstanceOf[SQLColumnDefinition]
      }.map { column =>

      val columnName = column.getName.getSimpleName
      val dataType = RawDBTypeToJavaType.convert(dbType, column.getDataType.getName)
      val isNullable = !column.containsNotNullConstaint()

      val fieldSize = extractfieldSize(column.getDataType) match {
        case Some(i) => i
        case None => 0
      }
      val fieldScale = 0

      val columnType = dialect.getCatalystType(dataType, column.getDataType.getName, fieldSize, new MetadataBuilder()).
        getOrElse(
          getCatalystType(dataType, fieldSize, fieldScale, false))

      StructField(columnName, columnType, isNullable)
    }
    new StructType(fields.toArray)

  }

  private def getCatalystType(
                               sqlType: Int,
                               precision: Int,
                               scale: Int,
                               signed: Boolean): DataType = {
    val answer = sqlType match {
      // scalastyle:off
      case java.sql.Types.ARRAY => null
      case java.sql.Types.BIGINT => if (signed) {
        LongType
      } else {
        DecimalType(20, 0)
      }
      case java.sql.Types.BINARY => BinaryType
      case java.sql.Types.BIT => BooleanType // @see JdbcDialect for quirks
      case java.sql.Types.BLOB => BinaryType
      case java.sql.Types.BOOLEAN => BooleanType
      case java.sql.Types.CHAR => StringType
      case java.sql.Types.CLOB => StringType
      case java.sql.Types.DATALINK => null
      case java.sql.Types.DATE => DateType
      case java.sql.Types.DECIMAL
        if precision != 0 || scale != 0 => DecimalType(min(precision, MAX_PRECISION), min(scale, MAX_SCALE))
      case java.sql.Types.DECIMAL => DecimalType.SYSTEM_DEFAULT
      case java.sql.Types.DISTINCT => null
      case java.sql.Types.DOUBLE => DoubleType
      case java.sql.Types.FLOAT => FloatType
      case java.sql.Types.INTEGER => if (signed) {
        IntegerType
      } else {
        LongType
      }
      case java.sql.Types.JAVA_OBJECT => null
      case java.sql.Types.LONGNVARCHAR => StringType
      case java.sql.Types.LONGVARBINARY => BinaryType
      case java.sql.Types.LONGVARCHAR => StringType
      case java.sql.Types.NCHAR => StringType
      case java.sql.Types.NCLOB => StringType
      case java.sql.Types.NULL => null
      case java.sql.Types.NUMERIC
        if precision != 0 || scale != 0 => DecimalType(min(precision, MAX_PRECISION), min(scale, MAX_SCALE))
      case java.sql.Types.NUMERIC => DecimalType.SYSTEM_DEFAULT
      case java.sql.Types.NVARCHAR => StringType
      case java.sql.Types.OTHER => null
      case java.sql.Types.REAL => DoubleType
      case java.sql.Types.REF => StringType
      case java.sql.Types.REF_CURSOR => null
      case java.sql.Types.ROWID => LongType
      case java.sql.Types.SMALLINT => IntegerType
      case java.sql.Types.SQLXML => StringType
      case java.sql.Types.STRUCT => StringType
      case java.sql.Types.TIME => TimestampType
      case java.sql.Types.TIME_WITH_TIMEZONE
      => null
      case java.sql.Types.TIMESTAMP => TimestampType
      case java.sql.Types.TIMESTAMP_WITH_TIMEZONE
      => null
      case java.sql.Types.TINYINT => IntegerType
      case java.sql.Types.VARBINARY => BinaryType
      case java.sql.Types.VARCHAR => StringType
      case _ =>
        throw new SQLException("Unrecognized SQL type " + sqlType)
      // scalastyle:on
    }

    if (answer == null) {
      throw new SQLException("Unsupported type " + JDBCType.valueOf(sqlType).getName)
    }
    answer
  }
}


