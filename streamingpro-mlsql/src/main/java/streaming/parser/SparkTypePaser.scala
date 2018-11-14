package streaming.parser

import org.apache.spark.sql.types._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.ScalaReflection
import streaming.common.SourceCodeCompiler

/**
  * Created by allwefantasy on 8/9/2018.
  */
object SparkTypePaser {
  private def findInputInArrayBracket(input: String) = {
    val max = input.length - 1
    val rest = ArrayBuffer[Char]()
    var firstS = false
    var fBracketCount = 0
    (0 until max).foreach { i =>
      input(i) match {
        case '(' =>
          if (firstS) {
            rest += input(i)
            fBracketCount += 1
          } else {
            firstS = true
          }
        case ')' => fBracketCount -= 1
          if (fBracketCount < 0) {
            firstS = false
          } else {
            rest += input(i)
          }
        case _ =>
          if (firstS) {
            rest += input(i)
          }

      }
    }
    rest.mkString("")
  }

  private def findKeyAndValue(input: String) = {
    val max = input.length - 1
    var fBracketCount = 0
    var position = 0
    (0 until max).foreach { i =>
      input(i) match {
        case '(' =>
          fBracketCount += 1
        case ')' =>
          fBracketCount -= 1
        case ',' =>
          if (fBracketCount == 0) {
            position = i
          }
        case _ =>
      }
    }
    (input.substring(0, position), input.substring(position + 1))
  }

  //array(array(map(string,string)))
  def toSparkType(dt: String): DataType = {
    dt match {
      case "boolean" => BooleanType
      case "byte" => ByteType
      case "short" => ShortType
      case "integer" => IntegerType
      case "date" => DateType
      case "long" => LongType
      case "float" => FloatType
      case "double" => DoubleType
      case "decimal" => DoubleType
      case "binary" => BinaryType
      case "string" => StringType
      case c: String if c.startsWith("array") =>
        ArrayType(toSparkType(findInputInArrayBracket(c)))
      case c: String if c.startsWith("map") =>
        //map(map(string,string),string)
        val (key, value) = findKeyAndValue(findInputInArrayBracket(c))
        MapType(toSparkType(key), toSparkType(value))

      case _ => throw new RuntimeException(s"$dt is not found spark type")

    }
  }

  def cleanSparkSchema(wowStructType: WowStructType): StructType = {
    StructType(wowStructType.list.map { field =>
      if (field.dataType.isInstanceOf[WowStructType]) {
        StructField(field.name, cleanSparkSchema(field.dataType.asInstanceOf[WowStructType]))
      } else {
        field
      }
    })
  }

  //st(field(name,string),field(name1,st(field(name2,array(string)))))
  def toSparkSchema(dt: String, st: WowStructType = WowStructType(ArrayBuffer[StructField]())): DataType = {
    def startsWith(c: String, token: String) = {
      c.startsWith(token) || c.startsWith(s"${token} ") || c.startsWith(s"${token}(")
    }
    dt match {
      case "boolean" => BooleanType
      case "byte" => ByteType
      case "short" => ShortType
      case "integer" => IntegerType
      case "date" => DateType
      case "long" => LongType
      case "float" => FloatType
      case "double" => DoubleType
      case "decimal" => DoubleType
      case "binary" => BinaryType
      case "string" => StringType

      case c: String if startsWith(c, "array") =>
        ArrayType(toSparkSchema(findInputInArrayBracket(c), st))
      case c: String if startsWith(c, "map") =>
        //map(map(string,string),string)
        val (key, value) = findKeyAndValue(findInputInArrayBracket(c))
        MapType(toSparkSchema(key, st), toSparkSchema(value, st))

      case c: String if startsWith(c, "st") =>
        val value = findInputInArrayBracket(c)
        val wst = WowStructType(ArrayBuffer[StructField]())
        toSparkSchema(value, wst)


      case c: String if startsWith(c, "field") =>
        val filedStrArray = ArrayBuffer[String]()
        findFieldArray(c, filedStrArray)

        filedStrArray.foreach { fs =>
          val (key, value) = findKeyAndValue(findInputInArrayBracket(fs))
          st.list += StructField(key, toSparkSchema(value, st))
        }
        st

      case _ => throw new RuntimeException("dt is not found spark schema")
    }
  }

  def findFieldArray(input: String, fields: ArrayBuffer[String]): Unit = {
    val max = input.length
    var fBracketCount = 0
    var position = 0
    var stop = false
    (0 until max).foreach { i =>
      if (!stop) {
        input(i) match {
          case '(' =>
            fBracketCount += 1
          case ')' =>
            fBracketCount -= 1
            if (i == max - 1 && fBracketCount == 0) {
              fields += input.substring(0, max)
            }
          case ',' =>
            if (fBracketCount == 0) {
              position = i
              fields += input.substring(0, position)
              findFieldArray(input.substring(position + 1), fields)
              stop = true
            }
          case _ =>
            if (i == max - 1 && fBracketCount == 0) {
              fields += input.substring(0, max + 1)
            }
        }
      }

    }
  }
}
