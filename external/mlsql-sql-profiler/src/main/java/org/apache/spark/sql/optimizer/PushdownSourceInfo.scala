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
package org.apache.spark.sql.optimizer

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.{Abs, Acos, Add, Alias, And, Ascii, Asin, Atan, Atan2, AttributeReference, Base64, Bin, BitwiseAnd, BitwiseNot, BitwiseOr, BitwiseXor, CaseWhen, Cast, Ceil, Coalesce, Concat, ConcatWs, Cos, Crc32, CurrentDate, CurrentTimestamp, DateAdd, DateDiff, DateFormatClass, DateSub, DayOfMonth, DayOfYear, Decode, Divide, Elt, Encode, EqualNullSafe, EqualTo, Exp, FindInSet, Floor, FromUnixTime, GreaterThan, GreaterThanOrEqual, Greatest, Hex, Hour, If, IfNull, In, IsNotNull, IsNull, LastDay, Least, Length, LessThan, LessThanOrEqual, Like, Literal, Log, Log10, Log2, Logarithm, Lower, Md5, Minute, Month, Multiply, NamedExpression, Not, NullIf, Or, ParseToDate, Pi, Pow, Quarter, RLike, Rand, Remainder, Round, Second, Sha1, Sha2, Signum, Sin, SoundEx, Sqrt, StringInstr, StringLPad, StringLocate, StringRPad, StringRepeat, StringSpace, StringTrim, StringTrimLeft, StringTrimRight, Substring, SubstringIndex, Subtract, Tan, ToDegrees, ToRadians, UnBase64, Unhex, UnixTimestamp, Upper, WeekOfYear, Year}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, _}
import org.apache.spark.sql.catalyst.sqlgenerator.LogicalPlanSQL
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCRelation}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import tech.mlsql.common.ScalaReflect
import tech.mlsql.indexer.impl.{KylinSQLDialect, MysqlSQLDialect}

import java.sql.DriverManager

abstract class PushdownSourceInfo(props: Map[String, String]){

}

object PushdownSourceInfo {

  //TODO : 用注册的方式

  def getPushdownSourceInfo(lr:LogicalRelation): PushdownSourceInfo ={
    lr.relation match {
      case l: JDBCRelation if (l.jdbcOptions.url.toLowerCase.startsWith("jdbc:mysql:")) =>
        val x= l.jdbcOptions.parameters.toMap
        new MysqlPushdownSourceInfo(l.jdbcOptions.parameters,l.sparkSession)
      case l: JDBCRelation if (l.jdbcOptions.url.toLowerCase.startsWith("jdbc:kylin:")) =>
        new KylinPushdownSourceInfo(l.jdbcOptions.parameters,l.sparkSession)
      case _ =>
        new NoPushdownSourceInfo(Map())
    }

  }

  def registSourceInfo():Unit= {
  }

}

class NoPushdownSourceInfo(props: Map[String, String]) extends PushdownSourceInfo(props){}
