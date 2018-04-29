package streaming.dsl.mmlib.algs

import scala.collection.mutable.ArrayBuffer

/**
  * Created by allwefantasy on 29/4/2018.
  */
object AnsjFunctions {
  def addWord(f: String, forest: AnyRef) = {
    val splits = f.split("\t")

    val paramsInDic = java.lang.reflect.Array.newInstance(classOf[String], 2)
    java.lang.reflect.Array.set(paramsInDic, 0, "userDefine")
    java.lang.reflect.Array.set(paramsInDic, 1, "1")

    if (splits.size == 2) {
      java.lang.reflect.Array.set(paramsInDic, 0, splits(1))
      java.lang.reflect.Array.set(paramsInDic, 1, "1")
    } else if (splits.size >= 3) {
      java.lang.reflect.Array.set(paramsInDic, 0, splits(1))
      java.lang.reflect.Array.set(paramsInDic, 1, splits(2))
    }

    forest.getClass.getMethod("add", classOf[String], classOf[Object]).invoke(forest, f, paramsInDic)
  }

  def configureDic(parser: AnyRef, forest: AnyRef, parserClassName: String, forestClassName: String) = {

    val forestsField = parser.getClass.getSuperclass.getDeclaredField("forests")
    forestsField.setAccessible(true)
    val forests = java.lang.reflect.Array.newInstance(Class.forName(forestClassName), 1)
    java.lang.reflect.Array.set(forests, 0, forest)
    forestsField.set(parser, forests)
  }

  def getTerm(item: AnyRef) = {
    val name = item.getClass.getMethod("getName").invoke(item)
    val nature = item.getClass.getMethod("getNatureStr").invoke(item)
    (name, nature)
  }

  def createForest(forestClassName:String) = {
    Class.forName(forestClassName).newInstance().asInstanceOf[AnyRef]
  }

  def createParser(parserClassName:String) = {
    Class.forName(parserClassName).newInstance().asInstanceOf[AnyRef]
  }


  def extractAllWords(forest:AnyRef,content:String) = {
    val udg = forest.getClass.getMethod("getWord", classOf[String]).invoke(forest, content.toLowerCase)
    def getAllWords(udg: Any) = {
      udg.getClass.getMethod("getAllWords").invoke(udg).asInstanceOf[String]
    }
    var tempWords = ArrayBuffer[String]()
    var temp = getAllWords(udg)
    while (temp != null) {
      tempWords += temp
      temp = getAllWords(udg)
    }
    tempWords
  }


}
