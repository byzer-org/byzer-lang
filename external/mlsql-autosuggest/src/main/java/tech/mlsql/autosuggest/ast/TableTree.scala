package tech.mlsql.autosuggest.ast

import tech.mlsql.autosuggest.meta.{MetaTable, MetaTableKey}
import tech.mlsql.autosuggest.statement.MetaTableKeyWrapper

import scala.collection.mutable.ArrayBuffer

object TableTree {
  def ROOT = {
    TableTree(-1, MetaTableKeyWrapper(MetaTableKey(None, None, null), None), None, ArrayBuffer())
  }

}

case class TableTree(level: Int, key: MetaTableKeyWrapper, table: Option[MetaTable], subNodes: ArrayBuffer[TableTree]) {


  def children = subNodes

  def collectByLevel(level: Int) = {
    val buffer = ArrayBuffer[TableTree]()
    visitDown(0) { case (table, _level) =>
      if (_level == level) {
        buffer += table
      }
    }
    buffer.toList
  }

  def visitDown(level: Int)(rule: PartialFunction[(TableTree, Int), Unit]): Unit = {
    rule.apply((this, level))
    this.children.map(_.visitDown(level + 1)(rule))
  }

  def visitUp(level: Int)(rule: PartialFunction[(TableTree, Int), Unit]): Unit = {
    this.children.map(_.visitUp(level + 1)(rule))
    rule.apply((this, level))
  }

  def fastEquals(other: TableTree): Boolean = {
    this.eq(other) || this == other
  }
}
