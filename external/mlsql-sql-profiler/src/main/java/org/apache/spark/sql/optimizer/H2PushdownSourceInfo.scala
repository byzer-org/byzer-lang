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

import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.{Abs, Acos, Add, Alias, And, Ascii, Asin, Atan, Atan2, AttributeReference, Base64, Bin, BitwiseAnd, BitwiseNot, BitwiseOr, BitwiseXor, CaseWhen, Cast, Ceil, Coalesce, Concat, ConcatWs, Cos, Crc32, CurrentDate, CurrentTimestamp, DateAdd, DateDiff, DateFormatClass, DateSub, DayOfMonth, DayOfYear, Decode, Divide, Elt, Encode, EqualNullSafe, EqualTo, Exp, FindInSet, Floor, FromUnixTime, GreaterThan, GreaterThanOrEqual, Greatest, Hex, Hour, If, IfNull, In, IsNotNull, IsNull, LastDay, Least, Length, LessThan, LessThanOrEqual, Like, Literal, Log, Log10, Log2, Logarithm, Lower, Md5, Minute, Month, Multiply, NamedExpression, Not, NullIf, Or, ParseToDate, Pi, Pow, Quarter, RLike, Rand, Remainder, Round, Second, Sha1, Sha2, Signum, Sin, SoundEx, Sqrt, StringInstr, StringLPad, StringLocate, StringRPad, StringRepeat, StringSpace, StringTrim, StringTrimLeft, StringTrimRight, Substring, SubstringIndex, Subtract, Tan, ToDegrees, ToRadians, UnBase64, Unhex, UnixTimestamp, Upper, WeekOfYear, Year}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.sqlgenerator.LogicalPlanSQL
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCRelation}
import org.apache.spark.sql.{DataFrame, SparkSession}
import tech.mlsql.common.ScalaReflect
import tech.mlsql.indexer.impl.H2SQLDialect


case class H2PushdownSourceInfo(props: Map[String, String], sparkSession: SparkSession, lr:LogicalRelation) extends PushdownSourceInfo(props) with Pushdownable {
  override val supportedOperators: Set[Class[_]] = Set(
    classOf[Project],
    classOf[Filter],
    classOf[Aggregate],
    classOf[Sort],
    classOf[Join],
    classOf[GlobalLimit],
    classOf[LocalLimit],
    classOf[Subquery],
    classOf[SubqueryAlias]
  )

  override val supportedUDF: Set[String] = Set()

  override val supportedExpressions: Set[Class[_]] = Set(
    classOf[Literal], classOf[AttributeReference], classOf[Alias], classOf[AggregateExpression],
    classOf[Abs], classOf[Coalesce], classOf[Greatest], classOf[If], classOf[IfNull],
    classOf[IsNull], classOf[IsNotNull], classOf[Least], classOf[NullIf],
    classOf[Rand], classOf[Acos], classOf[Asin], classOf[Atan],
    classOf[Atan2], classOf[Bin], classOf[Ceil], classOf[Cos], classOf[ToDegrees], classOf[Exp],
    classOf[Floor], classOf[Hex], classOf[Logarithm], classOf[Log10], classOf[Log2], classOf[Log],
    classOf[Pi], classOf[Pow], classOf[ToRadians], classOf[Round], classOf[Signum], classOf[Sin],
    classOf[Sqrt], classOf[Tan], classOf[Add], classOf[Subtract], classOf[Multiply], classOf[Divide],
    classOf[Remainder], classOf[Average], classOf[Count], classOf[Max], classOf[Min],
    classOf[StddevSamp], classOf[StddevPop], classOf[Sum], classOf[VarianceSamp], classOf[VariancePop],
    classOf[Ascii], classOf[Base64], classOf[Concat], classOf[ConcatWs],
    classOf[Decode], classOf[Elt], classOf[Encode], classOf[FindInSet], classOf[StringInstr],
    classOf[Lower], classOf[Length], classOf[Like], classOf[Lower], classOf[StringLocate],
    classOf[StringLPad], classOf[StringTrimLeft], classOf[StringRepeat], classOf[RLike],
    classOf[StringRPad], classOf[StringTrimRight], classOf[SoundEx], classOf[StringSpace],
    classOf[Substring], classOf[SubstringIndex], classOf[StringTrim], classOf[Upper], classOf[UnBase64],
    classOf[Unhex], classOf[Upper], classOf[CurrentDate], classOf[CurrentTimestamp], classOf[DateDiff],
    classOf[DateAdd], classOf[DateFormatClass], classOf[DateSub], classOf[DayOfMonth],
    classOf[DayOfYear], classOf[FromUnixTime], classOf[Hour], classOf[LastDay], classOf[Minute],
    classOf[Month], classOf[Quarter], classOf[Second], classOf[ParseToDate], classOf[UnixTimestamp],
    classOf[WeekOfYear], classOf[Year], classOf[Crc32], classOf[Md5], classOf[Sha1], classOf[Sha2],
    classOf[And], classOf[In], classOf[Not],
    classOf[Or], classOf[EqualNullSafe], classOf[EqualTo], classOf[GreaterThan],
    classOf[GreaterThanOrEqual], classOf[LessThan], classOf[LessThanOrEqual], classOf[Not], classOf[BitwiseAnd],
    classOf[BitwiseNot], classOf[BitwiseOr], classOf[BitwiseXor], classOf[Cast], classOf[CaseWhen]
  )

  override val beGoodAtOperators: Set[Class[_]] = Set(
    classOf[Join],
    classOf[GlobalLimit],
    classOf[LocalLimit],
    classOf[Aggregate]
  )

  override val supportedJoinTypes: Set[JoinType] = Set(
    Inner, Cross, LeftOuter, RightOuter
  )

  override def isSupportAll: Boolean = false

  override def canPushdown(lp: LogicalPlan):Boolean = {
    lp match {
      case l@LogicalRelation(jr@JDBCRelation(schema, parts, jdbcOptions), _, _, _) =>
        isTable(l) && isPushDown()
      case _ => false
    }
  }

  def isTable(lr:LogicalRelation):Boolean = {
    !ScalaReflect.fromInstance(lr.relation).field("jdbcOptions").invoke().asInstanceOf[JDBCOptions].tableOrQuery.toLowerCase.startsWith("select")
  }

  override def isPushDown():Boolean = {
    props.getOrElse("ispushdown","false").toBoolean
  }

  override def fastEquals(other: PushdownSourceInfo): Boolean = {
    other match {
      case mysql: H2PushdownSourceInfo =>
        connHostDBInfo == mysql.connHostDBInfo
      case _ => false
    }
  }

  private def connHostDBInfo():String ={
    val url = props.get("url").get
    url.split('?')(0)
  }

  override def buildScan(lp: LogicalPlan, sparkSession: SparkSession): DataFrame = ???

  override def buildScan(plan: LogicalPlan): DataFrame = ???

  override def buildScan2(lp: LogicalPlan, sparkSession: SparkSession): LogicalPlan = {
    val newqualifier = Seq.empty[String]
    val newlp1 = lp transform {
      case x:LogicalPlan => x transformExpressions {
        case c@Cast(car@AttributeReference(_, _, _, _), _, _) =>
          car.copy()(exprId = NamedExpression.newExprId, qualifier=car.qualifier)
        case c@Cast(ltr@Literal(_, _), _, _) =>
          ltr
        case ar@AttributeReference(_, _, _, _) =>
          ar.copy()(exprId = NamedExpression.newExprId, qualifier=ar.qualifier)
      }
    }

    // mlsql 生成的jdbc数据源的逻辑计划都会有这个子树
    val newlp2 = newlp1.transformDown{
      case sub@SubqueryAlias(name,pj@Project(pl, lr@LogicalRelation(_, _, _, _))) =>
        lr
      case sub@SubqueryAlias(name,lr@LogicalRelation(_, _, _, _)) =>
        lr
    }

    // TODO :这里要找一下更好的办法，目前这种方法可能不能列举所有的情况
    val newlp = newlp2.transformDown{
      case pj@Project(pl, lr@LogicalRelation(_, _, _, _)) =>
        pj transformExpressions {
          case car@AttributeReference(_, _, _, _) =>
            car.copy()(exprId = NamedExpression.newExprId, qualifier = newqualifier)
        }
      case agg@Aggregate(_,_, lr@LogicalRelation(_, _, _, _)) =>
        agg transformExpressions {
          case car@AttributeReference(_, _, _, _) =>
            car.copy()(exprId = NamedExpression.newExprId, qualifier = newqualifier)
        }
      case agg@Aggregate(_,_, fil@Filter(fc,lr@LogicalRelation(_, _, _, _))) =>
        agg transformExpressions {
          case car@AttributeReference(_, _, _, _) =>
            car.copy()(exprId = NamedExpression.newExprId, qualifier = newqualifier)
        }
      case fil@Filter(fc,lr@LogicalRelation(_, _, _, _)) =>
        fil transformExpressions {
          case car@AttributeReference(_, _, _, _) =>
            car.copy()(exprId = NamedExpression.newExprId, qualifier = newqualifier)
        }
    }

    val sqlBuilder = new LogicalPlanSQL(newlp, new H2SQLDialect)
    val sql = sqlBuilder.toSQL
    var newsub = newlp
    lp match {
      case sub1@SubqueryAlias(name1,child) =>
        val newr = createRelationFromOld(sql, lr.relation)
        val newlr = LogicalRelation(newr)
        newsub = SubqueryAlias(name1, newlr)
    }

    newsub
  }

  override def buildScan2(plan: LogicalPlan): LogicalPlan = {
    buildScan2(plan, sparkSession)
  }

}
