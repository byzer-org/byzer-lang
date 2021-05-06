package org.apache.spark.sql.optimizer

import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Join, Project, SubqueryAlias}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{ByteType, DataType, MetadataBuilder, StringType}
import org.scalatest.BeforeAndAfter
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.util.Utils

import java.sql.DriverManager
import java.util.Properties

class PushDownSuite extends QueryTest with BeforeAndAfter with SharedSQLContext {

  val url = "jdbc:h2:mem:testdb0"
  val urlWithUserAndPass = "jdbc:h2:mem:testdb0;user=testUser;password=testPass"
  var conn: java.sql.Connection = null

  before {
    Utils.classForName("org.h2.Driver")
    // Extra properties that will be specified for our database. We need these to test
    // usage of parameters from OPTIONS clause in queries.
    val properties = new Properties()
    properties.setProperty("user", "testUser")
    properties.setProperty("password", "testPass")
    properties.setProperty("rowId", "false")

    conn = DriverManager.getConnection(url, properties)
    conn.prepareStatement("create schema test").executeUpdate()

    conn.prepareStatement(
      "create table test.ssb_sales_1 (BUYER_ID INT, OPS_REGION VARCHAR(256), price DECIMAL(19,4))").executeUpdate()
    conn.prepareStatement("insert into test.ssb_sales_1 values (10000152,'Hongkong',60.1746)").executeUpdate()
    conn.prepareStatement("insert into test.ssb_sales_1 values (10000015,'Shanghai',0.155)").executeUpdate()
    conn.prepareStatement("insert into test.ssb_sales_1 values (10000017,'Beijing',13.1225)").executeUpdate()
    conn.prepareStatement("insert into test.ssb_sales_1 values (10000027,'Shanghai',86.2841)").executeUpdate()
    conn.prepareStatement("insert into test.ssb_sales_1 values (10000066,'Hongkong',83.3488)").executeUpdate()
    conn.prepareStatement("insert into test.ssb_sales_1 values (10000175,'Hongkong',31.4514)").executeUpdate()
    conn.prepareStatement("insert into test.ssb_sales_1 values (10000172,'Beijing',30.7342)").executeUpdate()
    conn.prepareStatement("insert into test.ssb_sales_1 values (10000074,'Beijing',88.0112)").executeUpdate()
    conn.prepareStatement("insert into test.ssb_sales_1 values (10000056,'Beijing',5.3982)").executeUpdate()
    conn.prepareStatement("insert into test.ssb_sales_1 values (10000116,'Hongkong',9.8382)").executeUpdate()

    conn.prepareStatement(
      "create table test.ssb_sales_2 (BUYER_ID INT, OPS_REGION VARCHAR(256), price DECIMAL(19,4))").executeUpdate()
    conn.prepareStatement("insert into test.ssb_sales_2 values (10000152,'Hongkong',60.1746)").executeUpdate()
    conn.prepareStatement("insert into test.ssb_sales_2 values (10000015,'Shanghai',0.155)").executeUpdate()
    conn.prepareStatement("insert into test.ssb_sales_2 values (10000017,'Beijing',13.1225)").executeUpdate()
    conn.prepareStatement("insert into test.ssb_sales_2 values (10000027,'Shanghai',86.2841)").executeUpdate()
    conn.prepareStatement("insert into test.ssb_sales_2 values (10000066,'Hongkong',83.3488)").executeUpdate()
    conn.prepareStatement("insert into test.ssb_sales_2 values (10000175,'Hongkong',31.4514)").executeUpdate()
    conn.prepareStatement("insert into test.ssb_sales_2 values (10000172,'Beijing',30.7342)").executeUpdate()
    conn.prepareStatement("insert into test.ssb_sales_2 values (10000074,'Beijing',88.0112)").executeUpdate()
    conn.prepareStatement("insert into test.ssb_sales_2 values (10000056,'Beijing',5.3982)").executeUpdate()
    conn.prepareStatement("insert into test.ssb_sales_2 values (10000116,'Hongkong',9.8382)").executeUpdate()

    conn.commit()

    sql(
      s"""
         |CREATE OR REPLACE TEMPORARY VIEW table_no_pushdown1
         |USING org.apache.spark.sql.jdbc
         |OPTIONS (url '$url', dbtable 'TEST.ssb_sales_1', user 'testUser', password 'testPass')
       """.stripMargin.replaceAll("\n", " "))

    sql(
      s"""
         |CREATE OR REPLACE TEMPORARY VIEW table_pushdown1
         |USING org.apache.spark.sql.jdbc
         |OPTIONS (url '$url', dbtable 'TEST.ssb_sales_1', user 'testUser', password 'testPass', ispushdown 'true')
       """.stripMargin.replaceAll("\n", " "))

    sql(
      s"""
         |CREATE OR REPLACE TEMPORARY VIEW table_pushdown2
         |USING org.apache.spark.sql.jdbc
         |OPTIONS (url '$url', dbtable 'TEST.ssb_sales_2', user 'testUser', password 'testPass', ispushdown 'true')
       """.stripMargin.replaceAll("\n", " "))

  }

  after {
    conn.close()
  }

  test("SELECT *") {
    assert(sql("SELECT * FROM table_no_pushdown1").collect().size === 10)
  }

  test("test h2 SQL push down"){
    //agg
    val lr1= Pushdown.apply(sql("SELECT sum(price) as sp FROM table_pushdown1").alias("test_view1").logicalPlan)
    assert( lr1 match {
      case sa@SubqueryAlias(name,lr@LogicalRelation(_, _, _, _)) =>
        true
      case _ => false
    })

    checkAnswer(lr1,Row(408.5182) :: Nil)

    //agg filter
    val lr2= Pushdown.apply(sql("SELECT sum(price) as sp FROM table_pushdown1 where OPS_REGION='Shanghai'").alias("test_view2").logicalPlan)
    assert( lr2 match {
      case sa@SubqueryAlias(name,lr@LogicalRelation(_, _, _, _)) =>
        true
      case _ => false
    })

    checkAnswer(lr2,Row(86.4391) :: Nil)

    //agg groupby
    val lr3 = Pushdown.apply(sql("SELECT BUYER_ID,sum(price) as ss from table_pushdown1 group by BUYER_ID").alias("test_view3").logicalPlan)
    assert( lr3 match {
      case sa@SubqueryAlias(name,lr@LogicalRelation(_, _, _, _)) =>
        true
      case _ => false
    })

    val expectedResult3 = Seq(
      (10000017,13.1225),
      (10000066,83.3488),
      (10000116, 9.8382),
      (10000152,60.1746),
      (10000056, 5.3982),
      (10000074,88.0112),
      (10000027,86.2841),
      (10000172,30.7342),
      (10000175,31.4514),
      (10000015, 0.1550)
    ).map { case (id, ss) =>
      Row(Integer.valueOf(id), BigDecimal.valueOf(ss))
    }

    checkAnswer(lr3,expectedResult3)

    //agg filter groupby
    val lr4 = Pushdown.apply(sql("SELECT BUYER_ID,sum(price) as ss from table_pushdown1 where OPS_REGION='Shanghai' group by BUYER_ID").alias("test_view4").logicalPlan)
    assert( lr4 match {
      case sa@SubqueryAlias(name,lr@LogicalRelation(_, _, _, _)) =>
        true
      case _ => false
    })

    val expectedResult4 = Seq(
      (10000015, 0.1550),
      (10000027, 86.2841)
    ).map { case (id, ss) =>
      Row(Integer.valueOf(id), BigDecimal.valueOf(ss))
    }
    checkAnswer(lr4,expectedResult4)

    //count(1)
    val lr5 = Pushdown.apply(sql("SELECT count(1) as cnt,sum(price) as ss from table_pushdown1 where OPS_REGION='Shanghai' group by OPS_REGION").alias("test_view5").logicalPlan)
    assert( lr5 match {
      case sa@SubqueryAlias(name,lr@LogicalRelation(_, _, _, _)) =>
        true
      case _ => false
    })

    val expectedResult5 = Seq(
      (2, 86.4391)
    ).map { case (id, ss) =>
      Row(Integer.valueOf(id), BigDecimal.valueOf(ss))
    }

    checkAnswer(lr5,expectedResult5)

    val expectedResult6 = Seq(
      (10000015, 0.1550, 0.1550),
      (10000027, 86.2841, 86.2841)
    ).map { case (id, ss, ss2) =>
      Row(Integer.valueOf(id), BigDecimal.valueOf(ss),BigDecimal.valueOf(ss2))
    }

    //join: push down root node
    val lr6 = Pushdown.apply(sql(
      """
        |select t1.BUYER_ID, t1.ss, t2.ss as s2 from
        |(select BUYER_ID,sum(price) as ss from table_pushdown1 where OPS_REGION="Shanghai" group by BUYER_ID) t1
        |join
        |(select BUYER_ID,sum(price) as ss from table_pushdown2 group by BUYER_ID) t2
        |on t1.BUYER_ID=t2.BUYER_ID
        |""".stripMargin).alias("test_view6").logicalPlan)
    assert( lr6 match {
      case sa@SubqueryAlias(name,lr@LogicalRelation(_, _, _, _)) =>
        true
      case _ => false
    })

    checkAnswer(lr6,expectedResult6)

    //join: push down one branch
    val lr7 = Pushdown.apply(sql(
      """
        |select t1.BUYER_ID, t1.ss, t2.ss as s2 from
        |(select BUYER_ID,sum(price) as ss from table_no_pushdown1 where OPS_REGION="Shanghai" group by BUYER_ID) t1
        |join
        |(select BUYER_ID,sum(price) as ss from table_pushdown1 group by BUYER_ID) t2
        |on t1.BUYER_ID=t2.BUYER_ID
        |""".stripMargin).alias("test_view6").logicalPlan)
    assert( lr7 match {
      case sa@SubqueryAlias(name,p@Project(_,join@Join(sal@SubqueryAlias(namel,agg@Aggregate(_,_,_)), sar@SubqueryAlias(namer,lrr@LogicalRelation(_, _, _, _)), _, _))) =>
        true
      case _ => false
    })

    checkAnswer(lr7,expectedResult6)

  }

}

