/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.sparkcube.optimizer

import scala.math.min

import com.swoop.alchemy.spark.expressions.hll.{HyperLogLogCardinality, HyperLogLogMerge}
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{CubeSharedState, SparkAgent, SparkSession}
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.DataWritingCommand
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import com.alibaba.sparkcube.CubeManager
import com.alibaba.sparkcube.catalog.{CacheInfo, CubeCacheInfo, RawCacheInfo}
import com.alibaba.sparkcube.conf.CubeConf


/**
 * Optimization rule to match plans
 */
case class GenPlanFromCache(session: SparkSession) extends Rule[LogicalPlan]
  with PredicateHelper {

  def cm: CubeManager = CubeSharedState.get(session).cubeManager

  def apply(plan: LogicalPlan): LogicalPlan =
    if (SQLConf.get.getConf(CubeConf.QUERY_REWRITE_ENABLED)) genPlan(plan) else plan

  def genPlan(plan: LogicalPlan): LogicalPlan = plan match {
    case dw: DataWritingCommand =>
      val dataPlan = treeMatching(dw.query)
      if (dataPlan.nonEmpty) {
        dw.withNewChildren(dataPlan)
      } else {
        plan
      }
    case _: Command =>
      plan // ignore other commands like create db, drop table ...
    case _ =>
      val matches = treeMatching(plan)
      if (matches.nonEmpty) {
        // TODO choose based on cost
        matches.head
      } else {
        plan
      }
  }

  def treeMatching(plan: LogicalPlan): Seq[LogicalPlan] = {
    val tryMatch = matchingPlan(plan).filter(validatePlan)
    if (tryMatch.nonEmpty) {
      // pre-order matching
      tryMatch
    } else {
      plan match {
        case p @ PhysicalOperation(fields, filters, child)
          if p != child =>
          def filtering(ch: LogicalPlan): LogicalPlan = if (filters.nonEmpty) {
            Filter(filters.reduce(And), ch)
          } else {
            ch
          }
          treeMatching(child).map(newChild => Project(fields, filtering(newChild)))
        case gl @ GlobalLimit(_, child) =>
          treeMatching(child).map(newPlan => gl.withNewChildren(Seq(newPlan)))
        case ll @ LocalLimit(_, child) =>
          treeMatching(child).map(newPlan => ll.withNewChildren(Seq(newPlan)))
        case j @ Join(left, right, _, _) =>
          (treeMatching(left), treeMatching(right)) match {
            case (Nil, planSeq) => planSeq.map(p => j.withNewChildren(Seq(left, p)))
            case (planSeq, Nil) => planSeq.map(p => j.withNewChildren(Seq(p, right)))
            case (leftPlanSeq, rightPlanSeq) =>
              leftPlanSeq.flatMap(lp => rightPlanSeq.map(rp => j.withNewChildren(Seq(lp, rp))))
            case _ => Nil
          }
        case a @ Aggregate(_, _, child) =>
          treeMatching(child).map(newPlan => a.withNewChildren(Seq(newPlan)))
        case u @ Union(children) =>
          val allMatching = children.map(oldPlan => treeMatching(oldPlan))
          if (allMatching.exists(_.nonEmpty)) {
            // TODO lose some potentials
            Seq(u.withNewChildren(children.zip(allMatching).map {
              case (old, matched) => matched.headOption.getOrElse(old)
            }))
          } else {
            Nil
          }
        case s @ Sort(_, true, child) =>
          treeMatching(child).map(newPlan => s.withNewChildren(Seq(newPlan)))
        case _ => Nil // Not supported operations yet
      }
    }
  }

  def matchingPlan(plan: LogicalPlan): Seq[LogicalPlan] = plan match {
    case Aggregate(_, _, Expand(_, _, PhysicalOperation(_, _, _))) =>
      // TODO real CUBE support
      Nil
    case Aggregate(ge, ae, p @ PhysicalOperation(fields, filters, t @ TableLike(ident)))
      if cm.isCached(session, ident.toString) =>
      val cacheOpt = cm.getViewCacheInfo(session, ident.toString)
      cacheOpt match {
        case Some(CacheInfo(_, Some(cube))) if cube.enableRewrite =>
          // for this kind of matching, we need everything from filters in dims,
          // and every ge in dims. Anything else should be in measures.
          val needs = ge.flatMap(_.references) ++ filters.flatMap(_.references)
          // TODO don't support actual project underneath this aggregate
          if (needs.forall(r => cube.cacheSchema.dims.contains(r.name)) && p.output == t.output) {
            scanCubePlan(
              cube, ident, plan.output, p.output, t.output, ge, ae, filters, fields).toSeq
          } else {
            Nil
          }
        case _ => Nil
      }
    case p @ PhysicalOperation(_, _, _) =>
      matchingRaw(p)
  }

  def matchingRaw(
      plan: LogicalPlan,
      requiredCols: Seq[String] = Seq("*")): Seq[LogicalPlan] = plan match {
    case TableLike(ident) if cm.isCached(session, ident.toString) =>
      val cacheOpt = cm.getViewCacheInfo(session, ident.toString)
      cacheOpt match {
        case Some(CacheInfo(Some(raw), _)) if raw.enableRewrite =>
          if (raw.cacheSchema.cols.contains("*") ||
            requiredCols.forall(raw.cacheSchema.cols.contains)) {
            scanRawPlan(raw, ident, plan.output).toSeq
          } else {
            // TODO if raw is not star but actually everything, and required is star
            // we should resolve star somewhere to keep this conformed.
            Nil
          }
        case _ => Nil
      }
    case p @ PhysicalOperation(fields, filters, child) if p != child =>
      val needs = fields.flatMap(_.references) ++ filters.flatMap(_.references)
      // TODO matching col in better way
      val matchedRaw = matchingRaw(child, needs.map(_.name.toLowerCase))
      val filtered = if (filters.nonEmpty) {
        val cond = filters.reduce(And)
        matchedRaw.map(Filter(cond, _))
      } else {
        matchedRaw
      }
      filtered.map(Project(fields, _))
    case _ => Nil
  }

  private def scanRawPlan(
      cache: RawCacheInfo, table: TableIdentifier, out: Seq[Attribute]): Option[LogicalPlan] = {
    val storage = cache.getStorageInfo
    // get metadata from view/table
    val meta = session.sessionState.catalog.getTableMetadata(table)
    // Note: partitionSchema is always empty in view, but actual data can still have partition
    // schema and a different data schema
    val allFields = meta.dataSchema
    val partitionSchema = StructType(storage.partitionSpec.getOrElse(Nil).map(pf =>
      allFields.find(f => pf.equalsIgnoreCase(f.name)).get))
    val allSchema = cache.cacheSchema.cols match {
      case Seq("*") => allFields.fields.toSeq
      case colSeq => colSeq.map(df =>
        StructField(df, allFields.find(_.name.equalsIgnoreCase(df)).get.dataType))
    }
    val dataSchema = StructType(allSchema.filterNot(s =>
      partitionSchema.fields.map(_.name).contains(s.name)))
    val formatClass = DataSource.lookupDataSource(storage.provider, SparkAgent.getConf(session))
    val format = formatClass.newInstance().asInstanceOf[FileFormat]
    val userSchemaOpt = Some(StructType(dataSchema.fields ++ partitionSchema.fields))
    val index = new InMemoryFileIndex(
      session, Seq(new Path(storage.storagePath)), Map.empty, userSchemaOpt)
    val lr = LogicalRelation(HadoopFsRelation(
      index, partitionSchema, dataSchema, storage.bucketSpec, format, Map.empty)(session), false)
    val base = Project(allSchema.map(sf =>
      Alias(lr.output.find(_.name.equalsIgnoreCase(sf.name)).get, sf.name)()), lr)
    // TODO matching col in better way
    val findMatch = out.map(o => base.output.find(bo => o.name.equalsIgnoreCase(bo.name)))
    if (findMatch.exists(_.isEmpty)) {
      None
    } else {
      // we need to bind reference here.
      val mapping =
        findMatch.flatten.zip(out).map(pair => Alias(pair._1, pair._2.name)(pair._2.exprId))
      Some(Project(mapping, base))
    }
  }

  private def scanCubePlan(
      cache: CubeCacheInfo, table: TableIdentifier,
      out: Seq[Attribute], phyOut: Seq[Attribute], tabOut: Seq[Attribute],
      grouping: Seq[Expression], aggregating: Seq[NamedExpression],
      filters: Seq[Expression], fields: Seq[NamedExpression]): Option[LogicalPlan] = {
    val filterRefs = filters.flatMap(_.references)
    // TODO matching col in better way
    val foundRefs = filterRefs.map(att =>
      cache.cacheSchema.dims.find(dim => dim.equalsIgnoreCase(att.name)))
    if (foundRefs.exists(_.isEmpty)) {
      None
    } else {
      val storage = cache.getStorageInfo
      // get metadata from view/table
      val meta = session.sessionState.catalog.getTableMetadata(table)
      // Note: partitionSchema is always empty in view, but actual data can still have partition
      // schema and a different data schema
      val allFields = meta.dataSchema
      // TODO matching col in better way
      val partitionSchema = StructType(storage.partitionSpec.getOrElse(Nil).map(pf =>
        allFields.find(f => pf.equalsIgnoreCase(f.name)).get))
      // TODO matching col in better way
      val allSchema = cache.cacheSchema.dims.map(df =>
        StructField(df, allFields.find(_.name.equalsIgnoreCase(df)).get.dataType)) ++
        cache.cacheSchema.measures.map(m =>
          StructField(m.name, getDataType(
            m.func, allFields.find(_.name.equalsIgnoreCase(m.column)).get.dataType)))
      val dataSchema = StructType(allSchema.filterNot(s =>
        partitionSchema.fields.map(_.name).contains(s.name)))
      val formatClass = DataSource.lookupDataSource(storage.provider, SparkAgent.getConf(session))
      val format = formatClass.newInstance().asInstanceOf[FileFormat]
      val userSchemaOpt = Some(StructType(dataSchema.fields ++ partitionSchema.fields))
      val index = new InMemoryFileIndex(
        session, Seq(new Path(storage.storagePath)), Map.empty, userSchemaOpt)
      val lr = LogicalRelation(HadoopFsRelation(
        index, partitionSchema, dataSchema, storage.bucketSpec, format, Map.empty)(session), false)
      // make sure the raw output is dims ... measures
      val raw = Project(allSchema.map(sf =>
        Alias(lr.output.find(_.name.equalsIgnoreCase(sf.name)).get, sf.name)()), lr)
      // TODO matching col in better way
      val replaceMap = tabOut.map(att => (att, raw.outputSet.find(rawAtt =>
        rawAtt.name.equalsIgnoreCase(att.name)))).filter(_._2.isDefined).map(p =>
        (p._1, p._2.get)).toMap
      val newFilters = filters.map(transformExpr(_, replaceMap))
      val base = if (newFilters.nonEmpty) {
        val cond = newFilters.reduce(And)
        Filter(cond, raw)
      } else {
        raw
      }
      // we are not doing project right here because the Project is underneath the replaced
      // Aggregate node, should carry it outside
      if (cache.cacheSchema.dims.forall(dim =>
        grouping.flatMap(_.references).exists(_.name.equalsIgnoreCase(dim))) &&
        grouping.forall(_.references.size == 1)) {
        // no need for re-aggr because the grouping are same
        val proj = aggregating.map(agg =>
          buildAggr(agg, base, cache.cacheSchema, reAggr = false).map(
            _.asInstanceOf[NamedExpression]))
        if (proj.forall(_.isDefined)) {
          val origin = Project(
            proj.flatten.map(transformExpr(_, replaceMap).asInstanceOf[NamedExpression]), base)
          val bounding =
            origin.output.zip(out).map(pair => Alias(pair._1, pair._2.name)(pair._2.exprId))
          Some(Project(bounding, origin))
        } else {
          None
        }
      } else {
        // need re-aggr
        val newAggrs = aggregating.map(agg =>
          buildAggr(agg, base, cache.cacheSchema, reAggr = true).map(
            transformExpr(_, replaceMap)).map(
            _.asInstanceOf[NamedExpression]))
        if (newAggrs.exists(_.isEmpty)) {
          None
        } else {
          val finalAggrPairs = newAggrs.flatten.map {
            exp =>
              val funcs = exp.collect { case f: FuncPlaceHolder => f }
              if (funcs.length > 1) {
                logDebug("Not support this kind of expression")
                return None
              }
              funcs.headOption match {
                case None => (None, Seq(exp))
                case Some(d @ DivideAfter(sume, cnte, _, _)) =>
                  (Some(ReDivide(exp)), Seq(
                    Alias(sume, d.name + "_sum")(),
                    Alias(cnte, d.name + "_cnt")()))
                case Some(c @ CardinalityAfter(buffer, _, _)) =>
                  (Some(ReCardinality(exp)), Seq(Alias(buffer, c.name + "_card")()))
              }
          }
          val firstAgg = Aggregate(
            grouping.map(ge => transformExpr(ge, replaceMap).asInstanceOf[NamedExpression]),
            finalAggrPairs.flatMap(_._2), base)
          val listExpr = finalAggrPairs.map { aggr =>
            aggr._1 match {
              case None => aggr._2.head.toAttribute
              case Some(ReDivide(originAttr)) =>
                val attrs = aggr._2.map(_.toAttribute)
                originAttr.transform {
                  case _: DivideAfter => attrs.head.dataType match {
                    case DoubleType =>
                      Divide(attrs(0), Cast(attrs(1), DoubleType))
                    case dec: DecimalType =>
                      Divide(attrs(0), Cast(attrs(1), dec))
                    case _ =>
                      Divide(Cast(attrs(0), DoubleType), Cast(attrs(1), DoubleType))
                  }
                }.asInstanceOf[NamedExpression]
              case Some(ReCardinality(originAttr)) =>
                val attrs = aggr._2.map(_.toAttribute)
                originAttr.transform {
                  case _: CardinalityAfter => HyperLogLogCardinality(attrs.head)
                }.asInstanceOf[NamedExpression]
              case e => throw new UnsupportedOperationException("Unknown expression:" + e)
            }
          }
          val bounding =
            listExpr.zip(out).map(pair => Alias(pair._1.toAttribute, pair._2.name)(pair._2.exprId))
          Some(Project(bounding, Project(listExpr, firstAgg)))
        }
      }
    }
  }

  private def transformExpr(
      projectItem: Expression,
      replaceMap: Map[Attribute, Attribute]): Expression = projectItem transform {
    case a: AttributeReference if replaceMap.contains(a) => replaceMap(a)
  }

  private def buildAggr(
      agg: Expression,
      base: LogicalPlan,
      cube: CacheCubeSchema,
      reAggr: Boolean): Option[Expression] = {
    agg match {
      case AggregateExpression(af, mode, isDistinct, resultId) =>
        (af, isDistinct) match {
          case (Count(Seq(exp)), true) if reAggr =>
            // count distinct re-aggr
            buildAggrExpr(exp, reAggr, mode, true, resultId, "pre_count_distinct", cube, base)
          case (Count(Seq(exp)), true) if !reAggr =>
            // count distinct
            val cntBuffer =
              buildAggrExpr(exp, reAggr, mode, false, resultId, "pre_count_distinct", cube, base)
            cntBuffer.map(BitSetCardinality)
          case (a @ Average(exp), false) =>
            // avg
            if (reAggr) {
              val sum = buildAggrExpr(exp, reAggr, mode, isDistinct, resultId, "sum", cube, base)
              val cnt = buildAggrExpr(exp, reAggr, mode, isDistinct, resultId, "count", cube, base)
              if (sum.isDefined && cnt.isDefined) {
                Some(DivideAfter(sum.get, cnt.get, a.nodeName))
              } else {
                None
              }
            } else {
              val avg = buildAggrExpr(exp, reAggr, mode, isDistinct, resultId, "avg", cube, base)
              if (avg.isEmpty) {
                val sume = buildAggrExpr(
                  exp, reAggr, mode, isDistinct, NamedExpression.newExprId, "sum", cube, base)
                val cnte = buildAggrExpr(
                  exp, reAggr, mode, isDistinct, NamedExpression.newExprId, "count", cube, base)
                if (sume.isDefined && cnte.isDefined) {
                  Some(sume.get.dataType match {
                    case DoubleType =>
                      Divide(sume.get, Cast(cnte.get, DoubleType))
                    case dec: DecimalType =>
                      Divide(sume.get, Cast(cnte.get, dec))
                    case _ =>
                      Divide(Cast(sume.get, DoubleType), Cast(cnte.get, DoubleType))
                  })
                } else {
                  None
                }
              } else {
                avg
              }
            }
          case (Sum(exp), false) =>
            buildAggrExpr(exp, reAggr, mode, isDistinct, resultId, "sum", cube, base)
          case (Count(exp), false) =>
            buildAggrExpr(exp.head, reAggr, mode, isDistinct, resultId, "count", cube, base)
          case (Min(exp), _) =>
            buildAggrExpr(exp, reAggr, mode, isDistinct, resultId, "min", cube, base)
          case (Max(exp), _) =>
            buildAggrExpr(exp, reAggr, mode, isDistinct, resultId, "max", cube, base)
          case (hll: HyperLogLogPlusPlus, _) =>
            val newHll = buildAggrExpr(
              hll.child, reAggr, mode, isDistinct,
              resultId, "pre_approx_count_distinct", cube, base, Some(hll))
            newHll match {
              case Some(AggregateExpression(_: HyperLogLogPlusPlus, _, _, _)) =>
                newHll
              case Some(other) =>
                if (reAggr) {
                  Some(CardinalityAfter(other, hll.toString))
                } else {
                  Some(HyperLogLogCardinality(other))
                }
              case _ => None
            }
        }
      case other if other.children.nonEmpty =>
        val childrenTrans = other.children.map(buildAggr(_, base, cube, reAggr))
        if (childrenTrans.exists(_.isDefined)) {
          Some(other.withNewChildren(childrenTrans.zip(other.children).map( pair =>
            pair._1.getOrElse(pair._2))))
        } else {
          None
        }
      case other: AttributeReference if cube.dims.exists(_.equalsIgnoreCase(other.name)) =>
        Some(other) // leaf node
      case _ => None
    }
  }

  private def findRefInBase(
      exp: Expression,
      funcName: String,
      cube: CacheCubeSchema,
      base: LogicalPlan): Option[(Expression, Boolean)] = {
    val oldRef = exp.references.toSeq
    val findingMap = oldRef.map { r =>
      val findFromMea = findMeasure(cube.measures, r, funcName)
      if (findFromMea < 0) {
        findDim(cube.dims, r)
      } else {
        findFromMea + cube.dims.length
      }
    }
    if (findingMap.forall(_ >= 0)) {
      val promo = findingMap.exists(_ >= cube.dims.length)
      Some((exp transform {
        case node if oldRef.contains(node) =>
          base.output(findingMap(oldRef.indexOf(node)))
      }, promo))
    } else {
      None
    }
  }

  private def buildAggrExpr(
      exp: Expression,
      reAggr: Boolean,
      mode: AggregateMode,
      isDistinct: Boolean,
      resultId: ExprId,
      funcName: String,
      cube: CacheCubeSchema,
      base: LogicalPlan,
      hll: Option[HyperLogLogPlusPlus] = None): Option[Expression] = {
    if (reAggr) {
      val findOpt = findRefInBase(exp, funcName, cube, base)
      findOpt.map(pair => AggregateExpression(buildAggrFunc(
        Seq(pair._1), funcName, pair._2, hll), mode, !pair._2 && isDistinct, resultId))
      // if promote, distinct is non-sense
    } else {
      val oldRef = exp.references.toSeq
      val findingMeasureMap = oldRef.map(r => findMeasure(cube.measures, r, funcName))
      if (findingMeasureMap.forall(_ >= 0)) {
        Some(exp transform {
          case node if oldRef.contains(node) =>
            base.output(findingMeasureMap(oldRef.indexOf(node)) + cube.dims.length)
        })
      } else {
        None
      }
    }
  }

  /*
   * not support calculate avg from sum and count here
   */
  private[optimizer] def buildAggrFunc(
      args: Seq[Expression],
      func: String,
      promo: Boolean,
      hll: Option[HyperLogLogPlusPlus] = None): AggregateFunction = func.toLowerCase match {
    case "max" => Max(args.head)
    case "min" => Min(args.head)
    case "count" if promo => Sum(args.head)
    case "count" if !promo => Count(args)
    case "sum" => Sum(args.head)
    case "avg" => Average(args.head)
    case "pre_count_distinct" if promo => ReCountDistinct(args.head)
    case "pre_count_distinct" if !promo => Count(args)
    case "pre_approx_count_distinct" if promo =>
      assert(hll.isDefined)
      val hllpp = hll.get
      HyperLogLogMerge(args.head, hllpp.mutableAggBufferOffset, hllpp.inputAggBufferOffset)
    case "pre_approx_count_distinct" if !promo =>
      assert(hll.isDefined)
      val hllpp = hll.get
      HyperLogLogPlusPlus(
        args.head, hllpp.relativeSD, hllpp.mutableAggBufferOffset, hllpp.inputAggBufferOffset)
    // case "pre_approx_count_distinct" if promo => PreApproxCountDistinct(args.head)
    // already handled pre_approx_count_distinct outside
    case _ => throw new UnsupportedOperationException("Unknown function") // should never be here
  }

  private def findMeasure(measures: Seq[Measure], a: Attribute, func: String): Int = {
    measures.indexWhere(m => m.column.equalsIgnoreCase(a.name) && m.func.equalsIgnoreCase(func))
  }

  private def findDim(dims: Seq[String], a: Attribute): Int = {
    dims.indexWhere(d => d.equalsIgnoreCase(a.name))
  }

  private def validatePlan(plan: LogicalPlan): Boolean = {
    val result = plan.map(_.missingInput.isEmpty).reduce(_ && _)
    if (!result) {
      logWarning(s"Invalid logical plan after cache based optimization.\n$plan")
    }
    result
  }

  private[optimizer] def getDataType(func: String, arg: DataType): DataType =
    (func.toLowerCase, arg) match {
      case ("count", _) => LongType
      case (fn, _) if Seq("pre_count_distinct", "pre_approx_count_distinct").contains(fn) =>
        BinaryType
      case (fn, _) if Seq("min", "max").contains(fn) => arg
      case ("sum", SparkAgent.DecimalResolve(precision, scale)) =>
        SparkAgent.createDecimal(precision + 10, scale)
      case ("sum", dt) if SparkAgent.isIntegral(dt) => LongType
      case ("sum", _) => DoubleType
      case ("avg", Fixed(p, s)) =>
        // DecimalType.bounded
        DecimalType(min(p + 4, DecimalType.MAX_PRECISION), min(s + 4, DecimalType.MAX_SCALE))
      case ("avg", _) => DoubleType
      case _ => throw new UnsupportedOperationException(s"unknown type")
    }

  private object Fixed {
    def unapply(t: DecimalType): Option[(Int, Int)] = Some((t.precision, t.scale))
  }
}

case class ReDivide(exp: NamedExpression)

case class ReCardinality(exp: NamedExpression)

/**
 * temporary place holder for average, will be replaced by Divide(sume, cnte)
 */
case class DivideAfter(
    sume: Expression,
    cnte: Expression,
    name: String,
    exprId: ExprId = NamedExpression.newExprId) extends FuncPlaceHolder {
  override def children: Seq[Expression] = Seq(sume, cnte)
}

/**
 * temporary place holder for [[HyperLogLogCardinality]]
 */
case class CardinalityAfter(
    buffer: Expression,
    name: String,
    exprId: ExprId = NamedExpression.newExprId) extends FuncPlaceHolder {
  override def children: Seq[Expression] = Seq(buffer)
}

sealed trait FuncPlaceHolder extends NamedExpression {
  override def qualifier: Seq[String] = Nil
  override def toAttribute: Attribute = throw new UnsupportedOperationException("")
  override def newInstance(): NamedExpression = throw new UnsupportedOperationException("")
  override def nullable: Boolean = true
  override def eval(input: InternalRow): Any = throw new UnsupportedOperationException("")
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    throw new UnsupportedOperationException("")
  override def dataType: DataType = throw new UnsupportedOperationException("")
}

object TableLike {
  def unapply(plan: LogicalPlan): Option[TableIdentifier] = plan match {
    case HiveTableRelation(tableMeta, _, _) =>
      Some(tableMeta.identifier)
    case View(catalogTable, _, _) =>
      Some(catalogTable.identifier)
    case SubqueryAlias(name, LogicalRelation(_, _, _, _)) =>
      Some(TableIdentifier(name.identifier,None))
    case _ => None
  }
}
