package tech.mlsql.sql

import com.alibaba.druid.sql.SQLUtils
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement
import com.alibaba.druid.sql.repository.SchemaRepository

import scala.collection.mutable

/**
  * Created by aston on 2019/5/27.
  */
object MLSQLSQLParser {
  def extractTableWithColumns(dbType :String ,sql :String ,createSchemaList :List[String]) = {
    val tableAndCols = mutable.HashMap.empty[String, mutable.HashSet[String]]

    val repository = new SchemaRepository(dbType)

    createSchemaList.foreach(repository.console(_))

    val stmtList = SQLUtils.parseStatements(sql, dbType)
    val stmt = stmtList.get(0).asInstanceOf[SQLSelectStatement]
    repository.resolve(stmt)

    val statVisitor = SQLUtils.createSchemaStatVisitor(dbType)
    stmt.accept(statVisitor)

    val iter = statVisitor.getColumns().iterator()

    while(iter.hasNext()){
      val c = iter.next()
      if(c.isSelect()){
        val value = tableAndCols.getOrElse(c.getTable, mutable.HashSet.empty[String])
        value.add(c.getName)
        tableAndCols.update(c.getTable, value)
      }
    }

    tableAndCols
  }
}