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
import java.lang.invoke.SerializedLambda
import java.util
import org.apache.spark.internal.Logging
import org.apache.spark.{SparkEnv, SparkException}
import org.apache.xbean.asm9.ClassReader

import scala.collection.mutable.{Map, Set, Stack}
import scala.language.existentials

/**
 * Created by allwefantasy on 6/8/2018.
 */
object WowCacheableClosureCleaner extends Logging {
  val serializableMap: LRUCache[String, Boolean] = new LRUCache[String, Boolean](100000)
  private val isScala2_11 = scala.util.Properties.versionString.contains("2.11")

  // Get an ASM class reader for a given class from the JAR that loaded it
  private[util] def getClassReader(cls: Class[_]): ClassReader = {
    // Copy data over, before delegating to ClassReader - else we can run out of open file handles.
    val className = cls.getName.replaceFirst("^.*\\.", "") + ".class"
    val resourceStream = cls.getResourceAsStream(className)
    if (resourceStream == null) {
      null
    } else {
      val baos = new ByteArrayOutputStream(128)
      Utils.copyStream(resourceStream, baos, true)
      new ClassReader(new ByteArrayInputStream(baos.toByteArray))
    }
  }

  // Check whether a class represents a Scala closure
  private def isClosure(cls: Class[_]): Boolean = {
    cls.getName.contains("$anonfun$")
  }

  // Get a list of the outer objects and their classes of a given closure object, obj;
  // the outer objects are defined as any closures that obj is nested within, plus
  // possibly the class that the outermost closure is in, if any. We stop searching
  // for outer objects beyond that because cloning the user's object is probably
  // not a good idea (whereas we can clone closure objects just fine since we
  // understand how all their fields are used).
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

  /**
   * Return a list of classes that represent closures enclosed in the given closure object.
   */
  private def getInnerClosureClasses(obj: AnyRef): List[Class[_]] = {
    val seen = Set[Class[_]](obj.getClass)
    val stack = Stack[Class[_]](obj.getClass)
    while (!stack.isEmpty) {
      val cr = getClassReader(stack.pop())
      if (cr != null) {
        val set = Set.empty[Class[_]]
        cr.accept(new InnerClosureFinder(set), 0)
        for (cls <- set -- seen) {
          seen += cls
          stack.push(cls)
        }
      }
    }
    (seen - obj.getClass).toList
  }

  /** Initializes the accessed fields for outer classes and their super classes. */
  private def initAccessedFields(
                                  accessedFields: Map[Class[_], Set[String]],
                                  outerClasses: Seq[Class[_]]): Unit = {
    for (cls <- outerClasses) {
      var currentClass = cls
      assert(currentClass != null, "The outer class can't be null.")

      while (currentClass != null) {
        accessedFields(currentClass) = Set.empty[String]
        currentClass = currentClass.getSuperclass()
      }
    }
  }

  /** Sets accessed fields for given class in clone object based on given object. */
  private def setAccessedFields(
                                 outerClass: Class[_],
                                 clone: AnyRef,
                                 obj: AnyRef,
                                 accessedFields: Map[Class[_], Set[String]]): Unit = {
    for (fieldName <- accessedFields(outerClass)) {
      val field = outerClass.getDeclaredField(fieldName)
      field.setAccessible(true)
      val value = field.get(obj)
      field.set(clone, value)
    }
  }

  /** Clones a given object and sets accessed fields in cloned object. */
  private def cloneAndSetFields(
                                 parent: AnyRef,
                                 obj: AnyRef,
                                 outerClass: Class[_],
                                 accessedFields: Map[Class[_], Set[String]]): AnyRef = {
    val clone = instantiateClass(outerClass, parent)

    var currentClass = outerClass
    assert(currentClass != null, "The outer class can't be null.")

    while (currentClass != null) {
      setAccessedFields(currentClass, clone, obj, accessedFields)
      currentClass = currentClass.getSuperclass()
    }

    clone
  }

  /**
   * Clean the given closure in place.
   *
   * More specifically, this renders the given closure serializable as long as it does not
   * explicitly reference unserializable objects.
   *
   * @param closure           the closure to clean
   * @param checkSerializable whether to verify that the closure is serializable after cleaning
   * @param cleanTransitively whether to clean enclosing closures transitively
   */
  def clean(
             closure: AnyRef,
             checkSerializable: Boolean = true,
             cleanTransitively: Boolean = true): Unit = {
    clean(closure, checkSerializable, cleanTransitively, Map.empty)
  }

  /**
   * Try to get a serialized Lambda from the closure.
   *
   * @param closure the closure to check.
   */
  private def getSerializedLambda(closure: AnyRef): Option[SerializedLambda] = {
    if (isScala2_11) {
      return None
    }
    val isClosureCandidate =
      closure.getClass.isSynthetic &&
        closure
          .getClass
          .getInterfaces.exists(_.getName == "scala.Serializable")

    if (isClosureCandidate) {
      try {
        Option(inspect(closure))
      } catch {
        case e: Exception =>
          // no need to check if debug is enabled here the Spark
          // logging api covers this.
          logDebug("Closure is not a serialized lambda.", e)
          None
      }
    } else {
      None
    }
  }

  private def inspect(closure: AnyRef): SerializedLambda = {
    val writeReplace = closure.getClass.getDeclaredMethod("writeReplace")
    writeReplace.setAccessible(true)
    writeReplace.invoke(closure).asInstanceOf[java.lang.invoke.SerializedLambda]
  }

  /**
   * Helper method to clean the given closure in place.
   *
   * The mechanism is to traverse the hierarchy of enclosing closures and null out any
   * references along the way that are not actually used by the starting closure, but are
   * nevertheless included in the compiled anonymous classes. Note that it is unsafe to
   * simply mutate the enclosing closures in place, as other code paths may depend on them.
   * Instead, we clone each enclosing closure and set the parent pointers accordingly.
   *
   * By default, closures are cleaned transitively. This means we detect whether enclosing
   * objects are actually referenced by the starting one, either directly or transitively,
   * and, if not, sever these closures from the hierarchy. In other words, in addition to
   * nulling out unused field references, we also null out any parent pointers that refer
   * to enclosing objects not actually needed by the starting closure. We determine
   * transitivity by tracing through the tree of all methods ultimately invoked by the
   * inner closure and record all the fields referenced in the process.
   *
   * For instance, transitive cleaning is necessary in the following scenario:
   *
   * class SomethingNotSerializable {
   * def someValue = 1
   * def scope(name: String)(body: => Unit) = body
   * def someMethod(): Unit = scope("one") {
   * def x = someValue
   * def y = 2
   * scope("two") { println(y + 1) }
   * }
   * }
   *
   * In this example, scope "two" is not serializable because it references scope "one", which
   * references SomethingNotSerializable. Note that, however, the body of scope "two" does not
   * actually depend on SomethingNotSerializable. This means we can safely null out the parent
   * pointer of a cloned scope "one" and set it the parent of scope "two", such that scope "two"
   * no longer references SomethingNotSerializable transitively.
   *
   * @param func              the starting closure to clean
   * @param checkSerializable whether to verify that the closure is serializable after cleaning
   * @param cleanTransitively whether to clean enclosing closures transitively
   * @param accessedFields    a map from a class to a set of its fields that are accessed by
   *                          the starting closure
   */
  private def clean(
                     func: AnyRef,
                     checkSerializable: Boolean,
                     cleanTransitively: Boolean,
                     accessedFields: Map[Class[_], Set[String]]): Unit = {

    // most likely to be the case with 2.12, 2.13
    // so we check first
    // non LMF-closures should be less frequent from now on
    val lambdaFunc = getSerializedLambda(func)

    if (!isClosure(func.getClass) && lambdaFunc.isEmpty) {
      logDebug(s"Expected a closure; got ${func.getClass.getName}")
      return
    }

    // TODO: clean all inner closures first. This requires us to find the inner objects.
    // TODO: cache outerClasses / innerClasses / accessedFields

    if (func == null) {
      return
    }

    if (lambdaFunc.isEmpty) {
      logDebug(s"+++ Cleaning closure $func (${func.getClass.getName}) +++")

      // A list of classes that represents closures enclosed in the given one
      val innerClasses = getInnerClosureClasses(func)

      // A list of enclosing objects and their respective classes, from innermost to outermost
      // An outer object at a given index is of type outer class at the same index
      val (outerClasses, outerObjects) = getOuterClassesAndObjects(func)

      // For logging purposes only
      val declaredFields = func.getClass.getDeclaredFields
      val declaredMethods = func.getClass.getDeclaredMethods

      if (log.isDebugEnabled) {
        logDebug(s" + declared fields: ${declaredFields.size}")
        declaredFields.foreach { f => logDebug(s"     $f") }
        logDebug(s" + declared methods: ${declaredMethods.size}")
        declaredMethods.foreach { m => logDebug(s"     $m") }
        logDebug(s" + inner classes: ${innerClasses.size}")
        innerClasses.foreach { c => logDebug(s"     ${c.getName}") }
        logDebug(s" + outer classes: ${outerClasses.size}")
        outerClasses.foreach { c => logDebug(s"     ${c.getName}") }
        logDebug(s" + outer objects: ${outerObjects.size}")
        outerObjects.foreach { o => logDebug(s"     $o") }
      }

      // Fail fast if we detect return statements in closures
      getClassReader(func.getClass).accept(new ReturnStatementFinder(), 0)

      // If accessed fields is not populated yet, we assume that
      // the closure we are trying to clean is the starting one
      if (accessedFields.isEmpty) {
        logDebug(" + populating accessed fields because this is the starting closure")
        // Initialize accessed fields with the outer classes first
        // This step is needed to associate the fields to the correct classes later
        initAccessedFields(accessedFields, outerClasses)

        // Populate accessed fields by visiting all fields and methods accessed by this and
        // all of its inner closures. If transitive cleaning is enabled, this may recursively
        // visits methods that belong to other classes in search of transitively referenced fields.
        for (cls <- func.getClass :: innerClasses) {
          getClassReader(cls).accept(new FieldAccessFinder(accessedFields, cleanTransitively), 0)
        }
      }

      logDebug(s" + fields accessed by starting closure: " + accessedFields.size)
      accessedFields.foreach { f => logDebug("     " + f) }

      // List of outer (class, object) pairs, ordered from outermost to innermost
      // Note that all outer objects but the outermost one (first one in this list) must be closures
      var outerPairs: List[(Class[_], AnyRef)] = outerClasses.zip(outerObjects).reverse
      var parent: AnyRef = null
      if (outerPairs.nonEmpty) {
        val (outermostClass, outermostObject) = outerPairs.head
        if (isClosure(outermostClass)) {
          logDebug(s" + outermost object is a closure, so we clone it: ${outerPairs.head}")
        } else if (outermostClass.getName.startsWith("$line")) {
          // SPARK-14558: if the outermost object is a REPL line object, we should clone
          // and clean it as it may carray a lot of unnecessary information,
          // e.g. hadoop conf, spark conf, etc.
          logDebug(s" + outermost object is a REPL line object, so we clone it: ${outerPairs.head}")
        } else {
          // The closure is ultimately nested inside a class; keep the object of that
          // class without cloning it since we don't want to clone the user's objects.
          // Note that we still need to keep around the outermost object itself because
          // we need it to clone its child closure later (see below).
          logDebug(" + outermost object is not a closure or REPL line object," +
            "so do not clone it: " + outerPairs.head)
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
        val clone = cloneAndSetFields(parent, obj, cls, accessedFields)

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

      logDebug(s" +++ closure $func (${func.getClass.getName}) is now cleaned +++")
    } else {
      logDebug(s"Cleaning lambda: ${lambdaFunc.get.getImplMethodName}")

      // scalastyle:off classforname
      val captClass = Class.forName(lambdaFunc.get.getCapturingClass.replace('/', '.'),
        false, Thread.currentThread.getContextClassLoader)
      // scalastyle:on classforname
      // Fail fast if we detect return statements in closures
      getClassReader(captClass)
        .accept(new ReturnStatementFinder(Some(lambdaFunc.get.getImplMethodName)), 0)
      logDebug(s" +++ Lambda closure (${lambdaFunc.get.getImplMethodName}) is now cleaned +++")
    }

    if (checkSerializable) {
      ensureSerializable(func)
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
}


case class LRUCache[K, V](cacheSize: Int) extends util.LinkedHashMap[K, V] {

  override def removeEldestEntry(eldest: util.Map.Entry[K, V]): Boolean = size > cacheSize

}

