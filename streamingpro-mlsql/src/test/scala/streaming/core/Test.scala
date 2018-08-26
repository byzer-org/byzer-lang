package streaming.core

import java.net.URL

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import org.scalatest.{FlatSpec, Matchers}
import streaming.common.{PunctuationUtils, UnicodeUtils}

import scala.collection.mutable

/**
  * Created by allwefantasy on 28/3/2017.
  */
object Test {
  def main(args: Array[String]): Unit = {
    // Create an RDD for the vertices
    //    String input = "07.Haz.2014";
    //
    //    java.util.Locale locale = new java.util.Locale( "tr", "TR" );
    //    DateTimeZone timeZone = DateTimeZone.forID( "Europe/Istanbul" );  // Assuming this time zone (not specified in Question).
    //    DateTimeFormatter formatter = DateTimeFormat.forPattern( "dd.MMM.yyyy" ).withLocale( locale ).withZone( timeZone );
    //    DateTime dateTime = formatter.parseDateTime( input );
    //    String outputQuébécois = DateTimeFormat.forStyle( "FF" ).withLocale( java.util.Locale.CANADA_FRENCH ).print( dateTime ); //
    //    DateTime dateTimeUtc = dateTime.withZone( DateTimeZone.UTC );
    val dt = ISODateTimeFormat.dateTime().parseDateTime("2018-08-26T08:28:07.051Z")
    println(dt.toString("yyyyMMdd HH:mm:ss"))

  }
}

case class VeterxAndGroup(vertexId: VertexId, group: VertexId)
