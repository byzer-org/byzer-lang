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

package org.apache.spark.sql

/**
  * Created by allwefantasy on 21/9/2018.
  */
/**
  * [[org.apache.spark.sql.Row]] enrichment functions
  */
object RichRow {

  implicit class RichRow(val row: Row) extends AnyVal {

    /**
      * Returns the value at position i. If the value is null, null is returned. The following
      * is a mapping between Spark SQL types and return types:
      *
      * {{{
      *   BooleanType -> java.lang.Boolean
      *   ByteType -> java.lang.Byte
      *   ShortType -> java.lang.Short
      *   IntegerType -> java.lang.Integer
      *   FloatType -> java.lang.Float
      *   DoubleType -> java.lang.Double
      *   StringType -> String
      *   DecimalType -> java.math.BigDecimal
      *
      *   DateType -> java.sql.Date
      *   TimestampType -> java.sql.Timestamp
      *
      *   BinaryType -> byte array
      *   ArrayType -> scala.collection.Seq (use getList for java.util.List)
      *   MapType -> scala.collection.Map (use getJavaMap for java.util.Map)
      *   StructType -> org.apache.spark.sql.Row
      * }}}
      */
    def getAny(fieldName: String): Any = row.get(row.fieldIndex(fieldName))

    /**
      * Returns map feature by name
      *
      * @param fieldName name of map feature
      * @return feature value as instance of Map[String, Any]
      */
    def getMapAny(fieldName: String): scala.collection.Map[String, Any] =
    row.getMap[String, Any](row.fieldIndex(fieldName))

    /**
      * Returns the value of field named {fieldName}. If the value is null, None is returned.
      */
    def getOptionAny(fieldName: String): Option[Any] = Option(getAny(fieldName))

    /**
      * Returns the value at position i. If the value is null, None is returned.
      */
    def getOptionAny(i: Integer): Option[Any] = Option(row.get(i))

    /**
      * Returns the value of field named {fieldName}. If the value is null, None is returned.
      */
    def getOption[T](fieldName: String): Option[T] = getOptionAny(fieldName) collect { case t: T@unchecked => t }

    /**
      * Returns the value at position i. If the value is null, None is returned.
      */
    def getOption[T](i: Integer): Option[T] = getOptionAny(i) collect { case t: T@unchecked => t }

  }

}
