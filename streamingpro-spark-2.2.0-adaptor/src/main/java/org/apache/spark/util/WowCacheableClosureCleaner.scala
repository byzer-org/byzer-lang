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

package org.apache.spark.util

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util

import org.apache.spark.internal.Logging
import org.apache.spark.{SparkEnv, SparkException}
import org.apache.xbean.asm5.ClassReader

import scala.collection.mutable.{Map, Set, Stack}
import scala.language.existentials
import scala.collection.mutable

/**
  * Created by allwefantasy on 6/8/2018.
  */
object WowCacheableClosureCleaner extends Logging {
  val serializableMap: LRUCache[String, Boolean] = new LRUCache[String, Boolean](100000)

  // Check whether a class represents a Scala closure
  private def isClosure(cls: Class[_]): Boolean = {
    cls.getName.contains("$anonfun$")
  }

  def clean(
             closure: AnyRef,
             checkSerializable: Boolean = true,
             cleanTransitively: Boolean = true): Unit = {
    clean(closure, checkSerializable, cleanTransitively, mutable.Map.empty)
  }

  private def clean(
                     func: AnyRef,
                     checkSerializable: Boolean,
                     cleanTransitively: Boolean,
                     accessedFields: mutable.Map[Class[_], mutable.Set[String]]): Unit = {

    if (!isClosure(func.getClass)) {
      logWarning("Expected a closure; got " + func.getClass.getName)
      return
    }

    // TODO: clean all inner closures first. This requires us to find the inner objects.
    // TODO: cache outerClasses / innerClasses / accessedFields

    if (func == null) {
      return
    }

    cleanFuncClosure(func, checkSerializable, cleanTransitively, accessedFields)

    if (checkSerializable) {
      ensureSerializable(func)
    }
  }

  private def cleanFuncClosure(
                                func: AnyRef,
                                checkSerializable: Boolean,
                                cleanTransitively: Boolean,
                                accessedFields: mutable.Map[Class[_], mutable.Set[String]]) = {
    // A list of classes that represents closures enclosed in the given one
    val innerClasses = getInnerClosureClasses(func)

    // A list of enclosing objects and their respective classes, from innermost to outermost
    // An outer object at a given index is of type outer class at the same index
    val (outerClasses, outerObjects) = getOuterClassesAndObjects(func)

    // Fail fast if we detect return statements in closures
    getClassReader(func.getClass).accept(new ReturnStatementFinder(), 0)

    // If accessed fields is not populated yet, we assume that
    // the closure we are trying to clean is the starting one
    if (accessedFields.isEmpty) {
      // Initialize accessed fields with the outer classes first
      // This step is needed to associate the fields to the correct classes later
      for (cls <- outerClasses) {
        accessedFields(cls) = Set[String]()
      }
      // Populate accessed fields by visiting all fields and methods accessed by this and
      // all of its inner closures. If transitive cleaning is enabled, this may recursively
      // visits methods that belong to other classes in search of transitively referenced fields.
      for (cls <- func.getClass :: innerClasses) {
        getClassReader(cls).accept(new FieldAccessFinder(accessedFields, cleanTransitively), 0)
      }
    }

    // List of outer (class, object) pairs, ordered from outermost to innermost
    // Note that all outer objects but the outermost one (first one in this list) must be closures
    var outerPairs: List[(Class[_], AnyRef)] = (outerClasses zip outerObjects).reverse
    var parent: AnyRef = null
    if (outerPairs.size > 0) {
      val (outermostClass, outermostObject) = outerPairs.head
      if (isClosure(outermostClass)) {

      } else if (outermostClass.getName.startsWith("$line")) {
        // SPARK-14558: if the outermost object is a REPL line object, we should clone and clean it
        // as it may carray a lot of unnecessary information, e.g. hadoop conf, spark conf, etc.

      } else {
        // The closure is ultimately nested inside a class; keep the object of that
        // class without cloning it since we don't want to clone the user's objects.
        // Note that we still need to keep around the outermost object itself because
        // we need it to clone its child closure later (see below).
        parent = outermostObject // e.g. SparkContext
        outerPairs = outerPairs.tail
      }
    } else {
      logDebug(" + there are no enclosing objects!")
    }

    // Clone the closure objects themselves, nulling out any fields that are not
    // used in the closure we're working on or any of its inner closures.
    for ((cls, obj) <- outerPairs) {
      logDebug(s" + cloning the object $obj of class ${cls.getName}")
      // We null out these unused references by cloning each object and then filling in all
      // required fields from the original object. We need the parent here because the Java
      // language specification requires the first constructor parameter of any closure to be
      // its enclosing object.
      val clone = instantiateClass(cls, parent)
      for (fieldName <- accessedFields(cls)) {
        val field = cls.getDeclaredField(fieldName)
        field.setAccessible(true)
        val value = field.get(obj)
        field.set(clone, value)
      }
      // If transitive cleaning is enabled, we recursively clean any enclosing closure using
      // the already populated accessed fields map of the starting closure
      if (cleanTransitively && isClosure(clone.getClass)) {
        logDebug(s" + cleaning cloned closure $clone recursively (${cls.getName})")
        // No need to check serializable here for the outer closures because we're
        // only interested in the serializability of the starting closure
        clean(clone, checkSerializable = false, cleanTransitively, accessedFields)
      }
      parent = clone
    }

    // Update the parent pointer ($outer) of this closure
    if (parent != null) {
      val field = func.getClass.getDeclaredField("$outer")
      field.setAccessible(true)
      // If the starting closure doesn't actually need our enclosing object, then just null it out
      if (accessedFields.contains(func.getClass) &&
        !accessedFields(func.getClass).contains("$outer")) {
        logDebug(s" + the starting closure doesn't actually need $parent, so we null it out")
        field.set(func, null)
      } else {
        // Update this closure's parent pointer to point to our enclosing object,
        // which could either be a cloned closure or the original user object
        field.set(func, parent)
      }
    }

  }

  private def ensureSerializable(func: AnyRef) {
    if (!serializableMap.containsKey(func.getClass.getCanonicalName)) {
      try {
        if (SparkEnv.get != null) {
          SparkEnv.get.closureSerializer.newInstance().serialize(func)
          serializableMap.put(func.getClass.getCanonicalName, true)
        }
      } catch {
        case ex: Exception => throw new SparkException("Task not serializable", ex)
      }
    }
  }

  private def instantiateClass(
                                cls: Class[_],
                                enclosingObject: AnyRef): AnyRef = {
    // Use reflection to instantiate object without calling constructor
    val rf = sun.reflect.ReflectionFactory.getReflectionFactory()
    val parentCtor = classOf[java.lang.Object].getDeclaredConstructor()
    val newCtor = rf.newConstructorForSerialization(cls, parentCtor)
    val obj = newCtor.newInstance().asInstanceOf[AnyRef]
    if (enclosingObject != null) {
      val field = cls.getDeclaredField("$outer")
      field.setAccessible(true)
      field.set(obj, enclosingObject)
    }
    obj
  }

  /**
    * Return a list of classes that represent closures enclosed in the given closure object.
    */
  private def getInnerClosureClasses(obj: AnyRef): List[Class[_]] = {
    val seen = Set[Class[_]](obj.getClass)
    val stack = Stack[Class[_]](obj.getClass)
    while (!stack.isEmpty) {
      val cr = getClassReader(stack.pop())
      val set = Set[Class[_]]()
      cr.accept(new InnerClosureFinder(set), 0)
      for (cls <- set -- seen) {
        seen += cls
        stack.push(cls)
      }
    }
    (seen - obj.getClass).toList
  }

  // Get an ASM class reader for a given class from the JAR that loaded it
  private[util] def getClassReader(cls: Class[_]): ClassReader = {
    // Copy data over, before delegating to ClassReader - else we can run out of open file handles.
    val className = cls.getName.replaceFirst("^.*\\.", "") + ".class"
    val resourceStream = cls.getResourceAsStream(className)
    // todo: Fixme - continuing with earlier behavior ...
    if (resourceStream == null) return new ClassReader(resourceStream)

    val baos = new ByteArrayOutputStream(128)
    Utils.copyStream(resourceStream, baos, true)
    new ClassReader(new ByteArrayInputStream(baos.toByteArray))
  }

  private def getOuterClassesAndObjects(obj: AnyRef): (List[Class[_]], List[AnyRef]) = {
    for (f <- obj.getClass.getDeclaredFields if f.getName == "$outer") {
      f.setAccessible(true)
      val outer = f.get(obj)
      // The outer pointer may be null if we have cleaned this closure before
      if (outer != null) {
        if (isClosure(f.getType)) {
          val recurRet = getOuterClassesAndObjects(outer)
          return (f.getType :: recurRet._1, outer :: recurRet._2)
        } else {
          return (f.getType :: Nil, outer :: Nil) // Stop at the first $outer that is not a closure
        }
      }
    }
    (Nil, Nil)
  }

  case class LRUCache[K, V](cacheSize: Int) extends util.LinkedHashMap[K, V] {

    override def removeEldestEntry(eldest: util.Map.Entry[K, V]): Boolean = size > cacheSize

  }

}
