/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package streaming.dsl.mmlib.algs

import java.sql.{Connection, SQLException}

import org.apache.spark.ml.param.Param
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import streaming.dsl.{ConnectMeta, DBMappingKey, ScriptSQLExec}
import org.apache.spark.sql.types._
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import tech.mlsql.job.JobManager

class SQLJDBCUpdatExt(override val uid: String) extends SQLAlg with WowParams{
  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]):DataFrame={
    params.get(keyCol.name).
      map(m => set(keyCol, m)).getOrElse {
      throw new MLSQLException("keyCol is required")
    }

    var _params = params
    if (path.contains(".")) {
      val Array(db, table) = path.split("\\.", 2)
      ConnectMeta.presentThenCall(DBMappingKey("jdbc", db), options => {
        options.foreach { item =>
          _params += (item._1 -> item._2)
        }
      })
    }
    val tableNameTemp = path.split("\\.").last
    if(tableNameTemp.isEmpty) throw new MLSQLException("target is required")
    var sql: String = s"UPDATE $tableNameTemp SET "
    var setTemp=""
    var whereTemp=""
    val columns = df.columns.map(x =>x.toUpperCase).toList
    var targetRDD = df.sparkSession.read
      .format(_params("format"))
      .option("url",_params("url"))
      .option("driver",_params("driver"))
      .option("user",_params("user"))
      .option("password",_params("password"))
      .option("dbtable", tableNameTemp).load()
    var columnsSchema=targetRDD.schema.fields.toList
    val keyColumns = params.get(keyCol.name).getOrElse().toString.toUpperCase.split(",").toList
    val expectKeyColumns = columns diff  keyColumns
    var columnsTotal = expectKeyColumns
    var flag_control = true
    expectKeyColumns.foreach(x =>{
      if(flag_control){
        setTemp=x+"=?"
        flag_control = false
      }
      else setTemp+=","+x+"=?"
    })
    flag_control = true
    keyColumns.foreach(x =>{
      columnsTotal=columnsTotal :+x
      if(flag_control){
        whereTemp=" where "+x+"=?"
        flag_control = false
      }
      else whereTemp+=" and "+x+"=?"
    })

    sql=sql+setTemp+whereTemp+" and "+params.get(whereCol.name).getOrElse("1=1")

    var df_change = df
    for (colName <- columns) {
      var columnSchema = columnsSchema.filter(p => p.name.toLowerCase==colName.toLowerCase)
      if(columnSchema.length<1)
      {
        throw new IllegalArgumentException(
          "target table Can't find field:"+colName
        )
      }
      df_change = df_change.withColumn(colName, df_change(colName).cast(columnSchema(0).dataType))
    }
    df_change.rdd.foreachPartition(f => {
       val connection = ConnectDB(_params)
       val stmt = connection.prepareStatement(sql)
       connection.setAutoCommit(false)
      while (f.hasNext) {
        val row = f.next()
        var order = 0;

        for (column <- columnsTotal) {
          order = order + 1
          var columnSchema = columnsSchema.filter(p => p.name.toLowerCase==column.toLowerCase)
          if(columnSchema.length<1)
            {
              throw new IllegalArgumentException(
                "target table Can't find field:"+column
              )
            }
          columnSchema(0).dataType match {
            case IntegerType => stmt.setInt(order, row.getAs[Int](column))
            case LongType => stmt.setLong(order, row.getAs[Long](column))
            case DoubleType => stmt.setDouble(order, row.getAs[Double](column))
            case FloatType => stmt.setFloat(order, row.getAs[Float](column))
            case ShortType => stmt.setInt(order, row.getAs[Short](column))
            case ByteType => stmt.setInt(order, row.getAs[Byte](column))
            case BooleanType => stmt.setBoolean(order, row.getAs[Boolean](column))
            case StringType => stmt.setString(order, row.getAs[String](column))
            case BinaryType => stmt.setBytes(order, row.getAs[Array[Byte]](column))
            case TimestampType => stmt.setTimestamp(order, row.getAs[java.sql.Timestamp](column))
            case DateType => stmt.setDate(order,row.getAs[java.sql.Date](column))
            case t: DecimalType => stmt.setBigDecimal(order,row.getAs[java.math.BigDecimal](column))
            case _ => throw new IllegalArgumentException(
              "Can't translate non-null value for field "+columnSchema(0).dataType
            )
          }
        }
        stmt.addBatch()
      }
      try{
        stmt.executeBatch()
        connection.commit()
      }catch{
        case ex: SQLException => {
          connection.rollback()
          ex.printStackTrace()
          throw new SQLException("Update Exception")
        }
      } finally {
        stmt.close()
        connection.close()
      }
    })

    import df.sparkSession.implicits._
    val context = ScriptSQLExec.context()
    val job = JobManager.getJobInfo(context.groupId)
    df.sparkSession.createDataset(Seq(job)).toDF()

  }

  def ConnectDB(options: Map[String, String]):Connection = {
    val driver = options("driver")
    val url = options("url")
    Class.forName(driver)
    val connection = java.sql.DriverManager.getConnection(url, options("user"), options("password"))
    connection
  }
  override def skipPathPrefix: Boolean = true
  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]) = throw new RuntimeException(s"${getClass.getName} not support load function.")

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]):UserDefinedFunction = {
    throw new RuntimeException(s"${getClass.getName} not support load function.")
  }
  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = train(df, path, params)
  final val keyCol: Param[String] = new Param[String](this, "keyCol", "")
  final val whereCol: Param[String] = new Param[String](this, "whereCol", "")

}
