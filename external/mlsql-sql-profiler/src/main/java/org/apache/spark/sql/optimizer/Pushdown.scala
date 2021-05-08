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

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, ExprId}
import org.apache.spark.sql.catalyst.plans.logical.{BinaryNode, Join, LeafNode, LogicalPlan, UnaryNode, Union}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import streaming.dsl.ScriptSQLExec
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.common.utils.serder.json.JSONTool

import scala.collection.mutable

object Pushdown extends Logging{
  def apply(lp: LogicalPlan): LogicalPlan = {
    val tree = tagTreeNode(lp)
    val replaceNodes = findPushdownNodes(tree)
    if(replaceNodes.nonEmpty){
      replacePushdownSubtree(tree,replaceNodes)
    }else lp
  }

  def tagTreeNode(lp: LogicalPlan):TagTreeNode = {
    val childrenTreeNode = lp.children.map(tagTreeNode)

    lp match {
      case lr:LogicalRelation =>
        val pds = PushdownSourceInfo.getPushdownSourceInfo(lr)
        pds match{
          case ds:Pushdownable if ds.canPushdown(lr)=>
            TagTreeNode(lr,pds,Nil,true)
          case _ => TagTreeNode(lr,new NoPushdownSourceInfo(Map()),Nil,false)
        }
      case lf:LeafNode =>
        TagTreeNode(lf, new NoPushdownSourceInfo(Map()),Nil,false)
      case ur: UnaryNode if childrenTreeNode.head.canPushDown =>
        childrenTreeNode.head.dataSource match {
          case pushdown: Pushdownable if pushdown.isSupport(ur) =>
            TagTreeNode(ur, childrenTreeNode.head.dataSource,childrenTreeNode,true)
          case _ =>
            TagTreeNode(ur, new NoPushdownSourceInfo(Map()),childrenTreeNode,false)
        }
      case jn:Join if childrenTreeNode.head.canPushDown && childrenTreeNode.tail.head.canPushDown=>
        val ld = childrenTreeNode.head.dataSource
        val rd = childrenTreeNode.tail.head.dataSource
        (ld,rd) match{
          case (l:Pushdownable,r:Pushdownable) if l.fastEquals(r) && l.isSupport(jn)=>
            TagTreeNode(jn, ld,childrenTreeNode,true)
          case _ =>
            TagTreeNode(jn, new NoPushdownSourceInfo(Map()),childrenTreeNode,false)
        }
      case bn:BinaryNode if childrenTreeNode.head.canPushDown && childrenTreeNode.tail.head.canPushDown =>
        val ld = childrenTreeNode.head.dataSource
        val rd = childrenTreeNode.tail.head.dataSource
        (ld,rd) match{
          case (l:Pushdownable,r:Pushdownable) if l.fastEquals(r) && l.isSupport(bn)=>
            TagTreeNode(bn, ld,childrenTreeNode,true)
          case _ =>
            TagTreeNode(bn, new NoPushdownSourceInfo(Map()),childrenTreeNode,false)
        }
      case un:Union if isChildrenCanPushdown(childrenTreeNode) =>
        if (isDataSourceEqual(childrenTreeNode) && childrenTreeNode.head.dataSource.asInstanceOf[Pushdownable].isSupport(un)){
          TagTreeNode(un, childrenTreeNode.head.dataSource,childrenTreeNode,true)
        }else{
          TagTreeNode(lp, new NoPushdownSourceInfo(Map()),childrenTreeNode,false)
        }
      case _ =>
        TagTreeNode(lp, new NoPushdownSourceInfo(Map()),childrenTreeNode,false)

    }

  }

  def findPushdownNodes(tree: TagTreeNode): mutable.HashSet[TagTreeNode] = {
    val pushdownNodes = new mutable.HashSet[TagTreeNode]()

    val toVisit = new mutable.Stack[TagTreeNode]()
    toVisit.push(tree)
    while(toVisit.nonEmpty){
      val visit = toVisit.pop()
      if (visit.canPushDown && visit.children!=Nil){
        pushdownNodes += visit
      }else{
        visit.children.foreach(child => toVisit.push(child))
      }
    }
    pushdownNodes
  }

  def replacePushdownSubtree(tree:TagTreeNode, replaceNodes:mutable.HashSet[TagTreeNode]): LogicalPlan ={
    val rootStartTime = System.currentTimeMillis()
    val enableLog = isEnableLog()

    val newlp = tree.lp.transformDown{
      case lp:LogicalPlan if replaceNodes.map(_.lp).contains(lp) =>
        val startTime = System.currentTimeMillis()
        try {
          val treeNode = replaceNodes.find(_.lp.equals(lp)).get
          val oldAttrs = lp.output
          val newsub = treeNode.dataSource.asInstanceOf[Pushdownable].buildScan2(lp)
          val newAttrs = newsub.output
          val newIdToOldId = mutable.Map.empty[ExprId, ExprId]
          newAttrs.zip(oldAttrs).foreach(elem => newIdToOldId += (elem._1.exprId -> elem._2.exprId))
          val newnewsub = newsub.transform {
            case x: LogicalPlan =>
              x.transformExpressions {
                case a: AttributeReference =>
                  if (newIdToOldId.contains(a.exprId)) {
                    a.copy()(exprId = newIdToOldId(a.exprId), a.qualifier)
                  } else a
              }
          }
          if(enableLog) {
            logInfo("----Old sub Logicplan:" + lp.toString())
            logInfo("----New sub Logicplan:" + newnewsub.toString())
          }
          newnewsub
        } catch {
          case ex:Exception =>
            logError("----Sub Logicplan Trans Error:"+ ex.getMessage)
            lp
        }finally{
          val endTime = System.currentTimeMillis()
          if(enableLog) {
            logInfo("----Sub Logicplan Trans Time Cost:" + (endTime - startTime) + "ms")
          }
        }
    }

    if(enableLog) {
      val rootEndTime = System.currentTimeMillis()
      logInfo("--Old Logicplan:" + tree.lp.toString())
      logInfo("--New Logicplan:" + tree.lp.toString())
      logInfo("--Logicplan Trans Time Cost:" + (rootEndTime - rootStartTime) + "ms")
    }
    newlp
  }

  private def isChildrenCanPushdown(children: Seq[TagTreeNode]):Boolean ={
    children.tail.forall{
      node =>
        (node.canPushDown, children.head.canPushDown) match {
          case (true,true) =>
            true
          case _ => false
        }
    }
  }

  private def isDataSourceEqual(children: Seq[TagTreeNode]): Boolean ={
    children.tail.forall { node =>
      (node.dataSource, children.head.dataSource) match {
        case (l: Pushdownable, r: Pushdownable) =>
          l.fastEquals(r)
        case _ => false
      }
    }
  }

  def getHttpParams()={
    JSONTool.parseJson[Map[String, String]](ScriptSQLExec.context().userDefinedParam.getOrElse("__PARAMS__", "{}"))
  }

  def getScriptParams()={
    ScriptSQLExec.context().execListener.env()
  }

  def isEnableLog(): Boolean ={
    try{
        val httpParams = getHttpParams()
        val scriptParams = getScriptParams()
        httpParams.getOrElse("enableQueryWithIndexer", "false").toBoolean ||
              scriptParams.getOrElse("enableQueryWithIndexer", "false").toBoolean
    }catch {
      case ex:Exception =>
        logError("----Get PARAMS Error:"+ ex.getMessage)
        false
    }
  }

}
