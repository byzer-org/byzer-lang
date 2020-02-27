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

  def createForest(forestClassName: String) = {
    Class.forName(forestClassName).newInstance().asInstanceOf[AnyRef]
  }

  def createParser(parserClassName: String) = {
    Class.forName(parserClassName).newInstance().asInstanceOf[AnyRef]
  }


  def extractAllWords(forest: AnyRef, content: String, deduplicateResult: Boolean) = {
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

    if (deduplicateResult) {
      tempWords.clear()
      tempWords.toSet[String].foreach(f => tempWords += f)
    }
    tempWords
  }


}
