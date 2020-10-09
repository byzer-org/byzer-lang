package tech.mlsql.lang.cmd.compile.internal.gc

import org.apache.commons.lang3.ClassUtils
import tech.mlsql.lang.cmd.compile.internal.gc.Types.DataType

import scala.collection.Map
import scala.reflect.ClassTag
import scala.util.control.NonFatal

private class MutableInt(var i: Int)

case class Origin(
                   line: Option[Int] = None,
                   startPosition: Option[Int] = None)

object CurrentOrigin {
  private val value = new ThreadLocal[Origin]() {
    override def initialValue: Origin = Origin()
  }

  def get: Origin = value.get()

  def set(o: Origin): Unit = value.set(o)

  def reset(): Unit = value.set(Origin())

  def setPosition(line: Int, start: Int): Unit = {
    value.set(
      value.get.copy(line = Some(line), startPosition = Some(start)))
  }

  def withOrigin[A](o: Origin)(f: => A): A = {
    set(o)
    val ret = try f finally {
      reset()
    }
    ret
  }
}

abstract class TreeNode[BaseType <: TreeNode[BaseType]] extends Product {
  self: BaseType =>
  val origin: Origin = CurrentOrigin.get

  /**
   * Returns a Seq of the children of this node.
   * Children should not change. Immutability required for containsChild optimization
   */
  def children: Seq[BaseType]

  lazy val containsChild: Set[TreeNode[_]] = children.toSet

  // Copied from Scala 2.13.1
  // github.com/scala/scala/blob/v2.13.1/src/library/scala/util/hashing/MurmurHash3.scala#L56-L73
  // to prevent the issue https://github.com/scala/bug/issues/10495
  // TODO(SPARK-30848): Remove this once we drop Scala 2.12.
  private final def productHash(x: Product, seed: Int, ignorePrefix: Boolean = false): Int = {
    val arr = x.productArity
    // Case objects have the hashCode inlined directly into the
    // synthetic hashCode method, but this method should still give
    // a correct result if passed a case object.
    if (arr == 0) {
      x.productPrefix.hashCode
    } else {
      var h = seed
      if (!ignorePrefix) h = scala.util.hashing.MurmurHash3.mix(h, x.productPrefix.hashCode)
      var i = 0
      while (i < arr) {
        h = scala.util.hashing.MurmurHash3.mix(h, x.productElement(i).##)
        i += 1
      }
      scala.util.hashing.MurmurHash3.finalizeHash(h, arr)
    }
  }

  private lazy val _hashCode: Int = productHash(this, scala.util.hashing.MurmurHash3.productSeed)

  override def hashCode(): Int = _hashCode

  /**
   * Faster version of equality which short-circuits when two treeNodes are the same instance.
   * We don't just override Object.equals, as doing so prevents the scala compiler from
   * generating case class `equals` methods
   */
  def fastEquals(other: TreeNode[_]): Boolean = {
    this.eq(other) || this == other
  }

  /**
   * Find the first [[TreeNode]] that satisfies the condition specified by `f`.
   * The condition is recursively applied to this node and all of its children (pre-order).
   */
  def find(f: BaseType => Boolean): Option[BaseType] = if (f(this)) {
    Some(this)
  } else {
    children.foldLeft(Option.empty[BaseType]) { (l, r) => l.orElse(r.find(f)) }
  }

  /**
   * Runs the given function on this node and then recursively on [[children]].
   *
   * @param f the function to be applied to each node in the tree.
   */
  def foreach(f: BaseType => Unit): Unit = {
    f(this)
    children.foreach(_.foreach(f))
  }

  /**
   * Runs the given function recursively on [[children]] then on this node.
   *
   * @param f the function to be applied to each node in the tree.
   */
  def foreachUp(f: BaseType => Unit): Unit = {
    children.foreach(_.foreachUp(f))
    f(this)
  }

  /**
   * Returns a Seq containing the result of applying the given function to each
   * node in this tree in a preorder traversal.
   *
   * @param f the function to be applied.
   */
  def map[A](f: BaseType => A): Seq[A] = {
    val ret = new collection.mutable.ArrayBuffer[A]()
    foreach(ret += f(_))
    ret.toSeq
  }

  /**
   * Returns a Seq by applying a function to all nodes in this tree and using the elements of the
   * resulting collections.
   */
  def flatMap[A](f: BaseType => TraversableOnce[A]): Seq[A] = {
    val ret = new collection.mutable.ArrayBuffer[A]()
    foreach(ret ++= f(_))
    ret.toSeq
  }

  /**
   * Returns a Seq containing the result of applying a partial function to all elements in this
   * tree on which the function is defined.
   */
  def collect[B](pf: PartialFunction[BaseType, B]): Seq[B] = {
    val ret = new collection.mutable.ArrayBuffer[B]()
    val lifted = pf.lift
    foreach(node => lifted(node).foreach(ret.+=))
    ret.toSeq
  }

  /**
   * Returns a Seq containing the leaves in this tree.
   */
  def collectLeaves(): Seq[BaseType] = {
    this.collect { case p if p.children.isEmpty => p }
  }

  /**
   * Finds and returns the first [[TreeNode]] of the tree for which the given partial function
   * is defined (pre-order), and applies the partial function to it.
   */
  def collectFirst[B](pf: PartialFunction[BaseType, B]): Option[B] = {
    val lifted = pf.lift
    lifted(this).orElse {
      children.foldLeft(Option.empty[B]) { (l, r) => l.orElse(r.collectFirst(pf)) }
    }
  }

  /**
   * Efficient alternative to `productIterator.map(f).toArray`.
   */
  protected def mapProductIterator[B: ClassTag](f: Any => B): Array[B] = {
    val arr = Array.ofDim[B](productArity)
    var i = 0
    while (i < arr.length) {
      arr(i) = f(productElement(i))
      i += 1
    }
    arr
  }

  /**
   * Returns a copy of this node with the children replaced.
   * TODO: Validate somewhere (in debug mode?) that children are ordered correctly.
   */
  def withNewChildren(newChildren: Seq[BaseType]): BaseType = {
    assert(newChildren.size == children.size, "Incorrect number of children")
    var changed = false
    val remainingNewChildren = newChildren.toBuffer
    val remainingOldChildren = children.toBuffer

    def mapTreeNode(node: TreeNode[_]): TreeNode[_] = {
      val newChild = remainingNewChildren.remove(0)
      val oldChild = remainingOldChildren.remove(0)
      if (newChild fastEquals oldChild) {
        oldChild
      } else {
        changed = true
        newChild
      }
    }

    def mapChild(child: Any): Any = child match {
      case arg: TreeNode[_] if containsChild(arg) => mapTreeNode(arg)
      // CaseWhen Case or any tuple type
      case (left, right) => (mapChild(left), mapChild(right))
      case nonChild: AnyRef => nonChild
      case null => null
    }

    val newArgs = mapProductIterator {
      case s: Stream[_] =>
        // Stream is lazy so we need to force materialization
        s.map(mapChild).force
      case s: Seq[_] =>
        s.map(mapChild)
      case m: Map[_, _] =>
        // `map.mapValues().view.force` return `Map` in Scala 2.12 but return `IndexedSeq` in Scala
        // 2.13, call `toMap` method manually to compatible with Scala 2.12 and Scala 2.13
        // `mapValues` is lazy and we need to force it to materialize
        m.mapValues(mapChild).view.force.toMap
      case arg: TreeNode[_] if containsChild(arg) => mapTreeNode(arg)
      case Some(child) => Some(mapChild(child))
      case nonChild: AnyRef => nonChild
      case null => null
      //      case s:_ => s
    }

    if (changed) makeCopy(newArgs) else this
  }

  /**
   * Returns a copy of this node where `rule` has been recursively applied to the tree.
   * When `rule` does not apply to a given node it is left unchanged.
   * Users should not expect a specific directionality. If a specific directionality is needed,
   * transformDown or transformUp should be used.
   *
   * @param rule the function use to transform this nodes children
   */
  def transform(rule: PartialFunction[BaseType, BaseType]): BaseType = {
    transformDown(rule)
  }

  /**
   * Returns a copy of this node where `rule` has been recursively applied to it and all of its
   * children (pre-order). When `rule` does not apply to a given node it is left unchanged.
   *
   * @param rule the function used to transform this nodes children
   */
  def transformDown(rule: PartialFunction[BaseType, BaseType]): BaseType = {
    val afterRule = CurrentOrigin.withOrigin(origin) {
      rule.applyOrElse(this, identity[BaseType])
    }

    // Check if unchanged and then possibly return old copy to avoid gc churn.
    if (this fastEquals afterRule) {
      mapChildren(_.transformDown(rule))
    } else {
      // If the transform function replaces this node with a new one, carry over the tags.
      //      afterRule.copyTagsFrom(this)
      afterRule.mapChildren(_.transformDown(rule))
    }
  }

  /**
   * Returns a copy of this node where `rule` has been recursively applied first to all of its
   * children and then itself (post-order). When `rule` does not apply to a given node, it is left
   * unchanged.
   *
   * @param rule the function use to transform this nodes children
   */
  def transformUp(rule: PartialFunction[BaseType, BaseType]): BaseType = {
    val afterRuleOnChildren = mapChildren(_.transformUp(rule))
    val newNode = if (this fastEquals afterRuleOnChildren) {
      CurrentOrigin.withOrigin(origin) {
        rule.applyOrElse(this, identity[BaseType])
      }
    } else {
      CurrentOrigin.withOrigin(origin) {
        rule.applyOrElse(afterRuleOnChildren, identity[BaseType])
      }
    }
    // If the transform function replaces this node with a new one, carry over the tags.
    //    newNode.copyTagsFrom(this)
    newNode
  }

  /**
   * Returns a copy of this node where `f` has been applied to all the nodes in `children`.
   */
  def mapChildren(f: BaseType => BaseType): BaseType = {
    if (containsChild.nonEmpty) {
      mapChildren(f, forceCopy = false)
    } else {
      this
    }
  }

  /**
   * Returns a copy of this node where `f` has been applied to all the nodes in `children`.
   *
   * @param f         The transform function to be applied on applicable `TreeNode` elements.
   * @param forceCopy Whether to force making a copy of the nodes even if no child has been changed.
   */
  private def mapChildren(
                           f: BaseType => BaseType,
                           forceCopy: Boolean): BaseType = {
    var changed = false

    def mapChild(child: Any): Any = child match {
      case arg: TreeNode[_] if containsChild(arg) =>
        val newChild = f(arg.asInstanceOf[BaseType])
        if (forceCopy || !(newChild fastEquals arg)) {
          changed = true
          newChild
        } else {
          arg
        }
      case tuple@(arg1: TreeNode[_], arg2: TreeNode[_]) =>
        val newChild1 = if (containsChild(arg1)) {
          f(arg1.asInstanceOf[BaseType])
        } else {
          arg1.asInstanceOf[BaseType]
        }

        val newChild2 = if (containsChild(arg2)) {
          f(arg2.asInstanceOf[BaseType])
        } else {
          arg2.asInstanceOf[BaseType]
        }

        if (forceCopy || !(newChild1 fastEquals arg1) || !(newChild2 fastEquals arg2)) {
          changed = true
          (newChild1, newChild2)
        } else {
          tuple
        }
      case other => other
    }

    val newArgs = mapProductIterator {
      case arg: TreeNode[_] if containsChild(arg) =>
        val newChild = f(arg.asInstanceOf[BaseType])
        if (forceCopy || !(newChild fastEquals arg)) {
          changed = true
          newChild
        } else {
          arg
        }
      case Some(arg: TreeNode[_]) if containsChild(arg) =>
        val newChild = f(arg.asInstanceOf[BaseType])
        if (forceCopy || !(newChild fastEquals arg)) {
          changed = true
          Some(newChild)
        } else {
          Some(arg)
        }
      // `map.mapValues().view.force` return `Map` in Scala 2.12 but return `IndexedSeq` in Scala
      // 2.13, call `toMap` method manually to compatible with Scala 2.12 and Scala 2.13
      case m: Map[_, _] => m.mapValues {
        case arg: TreeNode[_] if containsChild(arg) =>
          val newChild = f(arg.asInstanceOf[BaseType])
          if (forceCopy || !(newChild fastEquals arg)) {
            changed = true
            newChild
          } else {
            arg
          }
        case other => other
      }.view.force.toMap // `mapValues` is lazy and we need to force it to materialize
      case args: Stream[_] => args.map(mapChild).force // Force materialization on stream
      case args: Iterable[_] => args.map(mapChild)
      case nonChild: AnyRef => nonChild
      case null => null
      //      case s:AnyRef=> s
    }
    if (forceCopy || changed) makeCopy(newArgs, forceCopy) else this
  }

  /**
   * Args to the constructor that should be copied, but not transformed.
   * These are appended to the transformed args automatically by makeCopy
   *
   * @return
   */
  protected def otherCopyArgs: Seq[AnyRef] = Nil

  def attachTree[TreeType <: TreeNode[_], A](tree: TreeType, msg: String = "")(f: => A): A = {
    try f catch {
      // SPARK-16748: We do not want SparkExceptions from job failures in the planning phase
      // to create TreeNodeException. Hence, wrap exception only if it is not SparkException.
      case NonFatal(e) =>
        throw new TreeNodeException(tree, msg, e)
    }
  }

  /**
   * Creates a copy of this type of tree node after a transformation.
   * Must be overridden by child classes that have constructor arguments
   * that are not present in the productIterator.
   *
   * @param newArgs the new product arguments.
   */
  def makeCopy(newArgs: Array[AnyRef]): BaseType = makeCopy(newArgs, allowEmptyArgs = false)

  /**
   * Creates a copy of this type of tree node after a transformation.
   * Must be overridden by child classes that have constructor arguments
   * that are not present in the productIterator.
   *
   * @param newArgs        the new product arguments.
   * @param allowEmptyArgs whether to allow argument list to be empty.
   */
  private def makeCopy(
                        newArgs: Array[AnyRef],
                        allowEmptyArgs: Boolean): BaseType = attachTree(this, "makeCopy") {
    val allCtors = getClass.getConstructors
    if (newArgs.isEmpty && allCtors.isEmpty) {
      // This is a singleton object which doesn't have any constructor. Just return `this` as we
      // can't copy it.
      return this
    }

    // Skip no-arg constructors that are just there for kryo.
    val ctors = allCtors.filter(allowEmptyArgs || _.getParameterTypes.size != 0)
    if (ctors.isEmpty) {
      sys.error(s"No valid constructor for $nodeName")
    }
    val allArgs: Array[AnyRef] = if (otherCopyArgs.isEmpty) {
      newArgs
    } else {
      newArgs ++ otherCopyArgs
    }
    val defaultCtor = ctors.find { ctor =>
      if (ctor.getParameterTypes.length != allArgs.length) {
        false
      } else if (allArgs.contains(null)) {
        // if there is a `null`, we can't figure out the class, therefore we should just fallback
        // to older heuristic
        false
      } else {
        val argsArray: Array[Class[_]] = allArgs.map(_.getClass)
        ClassUtils.isAssignable(argsArray, ctor.getParameterTypes, true /* autoboxing */)
      }
    }.getOrElse(ctors.maxBy(_.getParameterTypes.length)) // fall back to older heuristic

    try {
      CurrentOrigin.withOrigin(origin) {
        val res = defaultCtor.newInstance(allArgs.toArray: _*).asInstanceOf[BaseType]
        //        res.copyTagsFrom(this)
        res
      }
    } catch {
      case e: java.lang.IllegalArgumentException =>
        throw new TreeNodeException(
          this,
          s"""
             |Failed to copy node.
             |Is otherCopyArgs specified correctly for $nodeName.
             |Exception message: ${e.getMessage}
             |ctor: $defaultCtor?
             |types: ${newArgs.map(_.getClass).mkString(", ")}
             |args: ${newArgs.mkString(", ")}
           """.stripMargin)
    }
  }

  override def clone(): BaseType = {
    mapChildren(_.clone(), forceCopy = true)
  }

  /**
   * Returns the name of this type of TreeNode.  Defaults to the class name.
   * Note that we remove the "Exec" suffix for physical operators here.
   */
  def nodeName: String = getClass.getSimpleName.replaceAll("Exec$", "")

  /**
   * The arguments that should be included in the arg string.  Defaults to the `productIterator`.
   */
  protected def stringArgs: Iterator[Any] = productIterator

  private lazy val allChildren: Set[TreeNode[_]] = (children ++ innerChildren).toSet[TreeNode[_]]
  /**
   * ONE line description of this node containing the node identifier.
   *
   * @return
   */
  //  def simpleStringWithNodeId(): String

  //  /** ONE line description of this node with more information */
  //  def verboseString(maxFields: Int): String
  //
  //  /** ONE line description of this node with some suffix information */
  //  def verboseStringWithSuffix(maxFields: Int): String = verboseString(maxFields)
  //
  //  override def toString: String = ""


  /**
   * Returns the tree node at the specified number, used primarily for interactive debugging.
   * Numbers for each node can be found in the [[numberedTreeString]].
   *
   * Note that this cannot return BaseType because logical plan's plan node might return
   * physical plan for innerChildren, e.g. in-memory relation logical plan node has a reference
   * to the physical plan node it is referencing.
   */
  def apply(number: Int): TreeNode[_] = getNodeNumbered(new MutableInt(number)).orNull

  /**
   * Returns the tree node at the specified number, used primarily for interactive debugging.
   * Numbers for each node can be found in the [[numberedTreeString]].
   *
   * This is a variant of [[apply]] that returns the node as BaseType (if the type matches).
   */
  def p(number: Int): BaseType = apply(number).asInstanceOf[BaseType]

  private def getNodeNumbered(number: MutableInt): Option[TreeNode[_]] = {
    if (number.i < 0) {
      None
    } else if (number.i == 0) {
      Some(this)
    } else {
      number.i -= 1
      // Note that this traversal order must be the same as numberedTreeString.
      innerChildren.map(_.getNodeNumbered(number)).find(_ != None).getOrElse {
        children.map(_.getNodeNumbered(number)).find(_ != None).flatten
      }
    }
  }

  /**
   * All the nodes that should be shown as a inner nested tree of this node.
   * For example, this can be used to show sub-queries.
   */
  def innerChildren: Seq[TreeNode[_]] = Seq.empty


  /**
   * Returns a 'scala code' representation of this `TreeNode` and its children.  Intended for use
   * when debugging where the prettier toString function is obfuscating the actual structure. In the
   * case of 'pure' `TreeNodes` that only contain primitives and other TreeNodes, the result can be
   * pasted in the REPL to build an equivalent Tree.
   */
  def asCode: String = {
    val args = productIterator.map {
      case tn: TreeNode[_] => tn.asCode
      case s: String => "\"" + s + "\""
      case other => other.toString
    }
    s"$nodeName(${args.mkString(",")})"
  }

}

class TreeNodeException[TreeType <: TreeNode[_]](
                                                  @transient val tree: TreeType,
                                                  msg: String,
                                                  cause: Throwable)
  extends Exception(msg, cause) {

  val treeString = tree.toString

  // Yes, this is the same as a default parameter, but... those don't seem to work with SBT
  // external project dependencies for some reason.
  def this(tree: TreeType, msg: String) = this(tree, msg, null)

  override def getMessage: String = {
    s"${super.getMessage}, tree:${if (treeString contains "\n") "\n" else " "}$tree"
  }
}

case class ExprCode(var code: String) 

abstract class Expression extends TreeNode[Expression] {
  def eval(ctx: CodegenContext): Any

  def genCode(ctx: CodegenContext): ExprCode

  lazy val resolved: Boolean = childrenResolved

  def childrenResolved: Boolean = children.forall(_.resolved)

  def dataType: Types.DataType

}

abstract class BinaryExpression extends Expression {
  def left: Expression

  def right: Expression

  override final def children: Seq[Expression] = Seq(left, right)
}

abstract class UnaryExpression extends Expression {

  def child: Expression

  override final def children: Seq[Expression] = child :: Nil
}

abstract class BinaryOperator extends BinaryExpression {

}

abstract class BinaryComparison extends BinaryOperator {

}

abstract class LeafExpression extends Expression {

  override final def children: Seq[Expression] = Nil
}

trait NamedExpression extends Expression {}


case class ParentGroup(expr: Expression) extends UnaryExpression {


  override def child: Expression = expr

  override def dataType: DataType = expr.dataType


  override def eval(ctx: CodegenContext): Any = {
    child.eval(ctx)
  }

  override def genCode(ctx: CodegenContext): ExprCode = {
    ExprCode(s"""(${child.genCode(ctx).code})""")
  }
}

case class Variable(name: String, dataType: Types.DataType) extends LeafExpression with NamedExpression {
  override def eval(ctx: CodegenContext): Any = {
    null
  }

  override def genCode(ctx: CodegenContext): ExprCode = {
    ExprCode(name.split(":").last)
  }
}

case class Literal(value: Any, dataType: Types.DataType) extends LeafExpression {
  override def eval(ctx: CodegenContext): Any = {
    dataType match {
      case Types.Int => value.toString.toInt
      case Types.Float => value.toString.toFloat
      case Types.String => value.toString
      case Types.Boolean => value.toString.toBoolean
      case Types.Any => value
    }
  }

  override def genCode(ctx: CodegenContext): ExprCode = {
    ExprCode(value.toString)
  }
}

case class Eql(left: Expression, right: Expression) extends BinaryComparison {
  override def dataType: DataType = Types.Boolean

  override def eval(ctx: CodegenContext): Any = {
    null
  }

  override def genCode(ctx: CodegenContext): ExprCode = {
    ExprCode(s"""${left.genCode(ctx).code} == ${right.genCode(ctx).code}""")
  }
}

case class Neq(left: Expression, right: Expression) extends BinaryComparison {
  override def dataType: DataType = Types.Boolean

  override def eval(ctx: CodegenContext): Any = {
    null
  }

  override def genCode(ctx: CodegenContext): ExprCode = {
    ExprCode(s"""${left.genCode(ctx).code} != ${right.genCode(ctx).code}""")
  }
}

case class Geq(left: Expression, right: Expression) extends BinaryComparison {
  override def dataType: DataType = Types.Boolean

  override def eval(ctx: CodegenContext): Any = {
    null
  }

  override def genCode(ctx: CodegenContext): ExprCode = {
    ExprCode(s"""${left.genCode(ctx).code} >= ${right.genCode(ctx).code}""")
  }
}

case class Gtr(left: Expression, right: Expression) extends BinaryComparison {
  override def dataType: DataType = Types.Boolean

  override def eval(ctx: CodegenContext): Any = {
    null
  }

  override def genCode(ctx: CodegenContext): ExprCode = {
    ExprCode(s"""${left.genCode(ctx).code} > ${right.genCode(ctx).code}""")
  }
}

case class Lss(left: Expression, right: Expression) extends BinaryComparison {
  override def dataType: DataType = Types.Boolean

  override def eval(ctx: CodegenContext): Any = {
    null
  }

  override def genCode(ctx: CodegenContext): ExprCode = {
    ExprCode(s"""${left.genCode(ctx).code} < ${right.genCode(ctx).code}""")
  }
}

case class Leq(left: Expression, right: Expression) extends BinaryComparison {
  override def dataType: DataType = Types.Boolean

  override def eval(ctx: CodegenContext): Any = {
    null
  }

  override def genCode(ctx: CodegenContext): ExprCode = {
    ExprCode(s"""${left.genCode(ctx).code} <= ${right.genCode(ctx).code}""")
  }
}

case class As(left: Expression, right: Expression) extends BinaryComparison {
  override def dataType: DataType = Types.Any

  override def eval(ctx: CodegenContext): Any = {
     left match {
       case a@Eql(_,_)=>
     }
  }

  override def genCode(ctx: CodegenContext): ExprCode = {
    ExprCode(s"""${right.genCode(ctx).code} as ${left.genCode(ctx).code}""")
  }
}



case class Add(left: Expression, right: Expression) extends BinaryComparison {
  override def dataType: DataType = Types.Float

  override def eval(ctx: CodegenContext): Any = {
    null
  }

  override def genCode(ctx: CodegenContext): ExprCode = {
    ExprCode(s"""${left.genCode(ctx).code} + ${right.genCode(ctx).code}""")
  }
}

case class Mul(left: Expression, right: Expression) extends BinaryComparison {
  override def dataType: DataType = Types.Float

  override def eval(ctx: CodegenContext): Any = {
    null
  }

  override def genCode(ctx: CodegenContext): ExprCode = {
    ExprCode(s"""${left.genCode(ctx).code} * ${right.genCode(ctx).code}""")
  }
}

case class Div(left: Expression, right: Expression) extends BinaryComparison {
  override def dataType: DataType = Types.Float

  override def eval(ctx: CodegenContext): Any = {
    null
  }

  override def genCode(ctx: CodegenContext): ExprCode = {
    ExprCode(s"""${left.genCode(ctx).code} / ${right.genCode(ctx).code}""")
  }
}

case class Rem(left: Expression, right: Expression) extends BinaryComparison {
  override def dataType: DataType = Types.Float

  override def eval(ctx: CodegenContext): Any = {
    null
  }

  override def genCode(ctx: CodegenContext): ExprCode = {
    ExprCode(s"""${left.genCode(ctx).code} % ${right.genCode(ctx).code}""")
  }
}

case class Sub(left: Expression, right: Expression) extends BinaryComparison {
  override def dataType: DataType = Types.Float

  override def eval(ctx: CodegenContext): Any = {
    null
  }

  override def genCode(ctx: CodegenContext): ExprCode = {
    ExprCode(s"""${left.genCode(ctx).code} - ${right.genCode(ctx).code}""")
  }
}

case class Cast(left: Expression, right: Expression) extends BinaryComparison {
  override def dataType: DataType = Types.Float

  override def eval(ctx: CodegenContext): Any = {
    null
  }

  override def genCode(ctx: CodegenContext): ExprCode = {
    ExprCode(s"""cast(${left.genCode(ctx).code} as ${right.genCode(ctx).code})""")
  }
}

case class AndAnd(left: Expression, right: Expression) extends BinaryOperator {
  override def dataType: DataType = Types.Boolean

  override def eval(ctx: CodegenContext): Any = {
    null
  }

  override def genCode(ctx: CodegenContext): ExprCode = {
    ExprCode(s"""${left.genCode(ctx).code} and ${right.genCode(ctx).code}""")
  }
}

case class OrOr(left: Expression, right: Expression) extends BinaryOperator {
  override def dataType: DataType = Types.Boolean

  override def eval(ctx: CodegenContext): Any = {
    null
  }

  override def genCode(ctx: CodegenContext): ExprCode = {
    ExprCode(s"""${left.genCode(ctx).code} or ${right.genCode(ctx).code}""")
  }
}

case class ArrayIndexer(left: Expression, right: Expression) extends BinaryOperator {
  override def dataType: DataType = Types.Any

  override def eval(ctx: CodegenContext): Any = {
    null
  }

  override def genCode(ctx: CodegenContext): ExprCode = {
    ExprCode(s"""${left.genCode(ctx).code}[${right.genCode(ctx).code}]""")
  }
}

case class FuncCall(left: Literal, right: Seq[Expression]) extends Expression {
  override def dataType: DataType = Types.Any

  override def children: Seq[Expression] = right

  override def eval(ctx: CodegenContext): Any = {
    null
  }

  override def genCode(ctx: CodegenContext): ExprCode = {
    ExprCode(s"""${left.genCode(ctx).code}(${right.map(_.genCode(ctx).code).mkString(",")})""")
  }
}

case class Select(items: Seq[As]) extends Expression {
  override def dataType: DataType = Types.Any

  /**
   * Returns a Seq of the children of this node.
   * Children should not change. Immutability required for containsChild optimization
   */
  override def children: Seq[Expression] = items

  override def eval(ctx: CodegenContext): Any = {
    null
  }

  override def genCode(ctx: CodegenContext): ExprCode = {
    ExprCode(s"""select ${items.map(_.genCode(ctx).code).mkString(",")}""")
  }
}


abstract class QueryPlan[PlanType <: QueryPlan[PlanType]] extends TreeNode[PlanType] {
  self: PlanType =>
  /**
   * Runs [[transformExpressionsDown]] with `rule` on all expressions present
   * in this query operator.
   * Users should not expect a specific directionality. If a specific directionality is needed,
   * transformExpressionsDown or transformExpressionsUp should be used.
   *
   * @param rule the rule to be applied to every expression in this operator.
   */
  def transformExpressions(rule: PartialFunction[Expression, Expression]): this.type = {
    transformExpressionsDown(rule)
  }

  /**
   * Runs [[transformDown]] with `rule` on all expressions present in this query operator.
   *
   * @param rule the rule to be applied to every expression in this operator.
   */
  def transformExpressionsDown(rule: PartialFunction[Expression, Expression]): this.type = {
    mapExpressions(_.transformDown(rule))
  }

  /**
   * Apply a map function to each expression present in this query operator, and return a new
   * query operator based on the mapped expressions.
   */
  def mapExpressions(f: Expression => Expression): this.type = {
    var changed = false

    @inline def transformExpression(e: Expression): Expression = {
      val newE = CurrentOrigin.withOrigin(e.origin) {
        f(e)
      }
      if (newE.fastEquals(e)) {
        e
      } else {
        changed = true
        newE
      }
    }

    def recursiveTransform(arg: Any): AnyRef = arg match {
      case e: Expression => transformExpression(e)
      case Some(value) => Some(recursiveTransform(value))
      case m: Map[_, _] => m
      case stream: Stream[_] => stream.map(recursiveTransform).force
      case seq: Iterable[_] => seq.map(recursiveTransform)
      case other: AnyRef => other
      case null => null
    }

    val newArgs = mapProductIterator(recursiveTransform)

    if (changed) makeCopy(newArgs).asInstanceOf[this.type] else this
  }

  /**
   * Returns the result of running [[transformExpressions]] on this node
   * and all its children. Note that this method skips expressions inside subqueries.
   */
  def transformAllExpressions(rule: PartialFunction[Expression, Expression]): this.type = {
    transform {
      case q: QueryPlan[_] => q.transformExpressions(rule).asInstanceOf[PlanType]
    }.asInstanceOf[this.type]
  }

  /** Returns all of the expressions present in this query plan operator. */
  final def expressions: Seq[Expression] = {
    // Recursively find all expressions from a traversable.
    def seqToExpressions(seq: Iterable[Any]): Iterable[Expression] = seq.flatMap {
      case e: Expression => e :: Nil
      case s: Iterable[_] => seqToExpressions(s)
      case other => Nil
    }

    productIterator.flatMap {
      case e: Expression => e :: Nil
      case s: Some[_] => seqToExpressions(s.toSeq)
      case seq: Iterable[_] => seqToExpressions(seq)
      case other => Nil
    }.toSeq
  }

}