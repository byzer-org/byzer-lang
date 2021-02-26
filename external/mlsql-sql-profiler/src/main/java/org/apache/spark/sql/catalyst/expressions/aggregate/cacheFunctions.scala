/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.expressions.aggregate

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import org.roaringbitmap.RoaringBitmap

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._


// this file stays in package spark because AbstractDataType TypeCollection is private to spark
/**
 * Generate a Bitset with input integers, return the serialized Bitset, it could be used for further
 * re_count_distinct.
 */
@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Returns the pre-aggregated distinct data in bitset.
  """)
case class BitSetMapping(
    child: Expression,
    override val mutableAggBufferOffset: Int = 0,
    override val inputAggBufferOffset: Int = 0)
  extends ImperativeAggregateWithRoaringBitmap with ImplicitCastInputTypes {

  def this(child: Expression) = {
    this(child = child, mutableAggBufferOffset = 0,
      inputAggBufferOffset = 0)
  }

  override def prettyName: String = "bit_set_mapping"

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def children: Seq[Expression] = Seq(child)

  override def nullable: Boolean = false

  override def dataType: DataType = BinaryType

  override def createAggregationBuffer(): RoaringBitmap = {
    new RoaringBitmap
  }

  override def update(bitSet: RoaringBitmap, input: InternalRow): RoaringBitmap = {
    val v = child.eval(input)
    if (v != null) {
      bitSet.add(v.asInstanceOf[Int])
      bitSet
    } else {
      bitSet
    }
  }

  override def merge(
      one: RoaringBitmap,
      other: RoaringBitmap): RoaringBitmap = {
    one.or(other)
    one
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(IntegerType)

  override def eval(bitSet: RoaringBitmap): Any = {
    serialize(bitSet)
  }
}

/**
 * Generate a Bitset which contains all the distinct encoded values, so that its result can be used
 * to do re-aggregation for COUNT DISTINCT. It's just a placeholder, as PreCountDistinctTransformer
 * rule would rewrite this with BitSetMapping and DictionaryEncode. This function should only be
 * used in CACHE TABLE DDL.
 */
@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Just a placeholder, should be replaced with DictionEncoding and
    BitSetMapping in optimizer.
  """)
case class PreCountDistinct(
    child: Expression) extends ImperativeAggregate with ImplicitCastInputTypes {

  var dictionaryPath: String = _

  override def prettyName: String = "pre_count_distinct"

  override protected val mutableAggBufferOffset: Int = 0

  override protected val inputAggBufferOffset: Int = 0

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    this

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    this

  override def initialize(mutableAggBuffer: InternalRow): Unit = {}

  override def update(mutableAggBuffer: InternalRow, inputRow: InternalRow): Unit = {}

  override def merge(mutableAggBuffer: InternalRow, inputAggBuffer: InternalRow): Unit = {}

  override def aggBufferSchema: StructType = StructType.fromAttributes(aggBufferAttributes)

  override def aggBufferAttributes: Seq[AttributeReference] = {Seq.empty[AttributeReference]}

  override def inputAggBufferAttributes: Seq[AttributeReference] = {Seq.empty[AttributeReference]}

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(StringType, BinaryType, NumericType))

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {}

  override def dataType: DataType = BinaryType

  override def children: Seq[Expression] = Seq(child)
}

/**
 * Take the BitSet as input, and re-aggregate the final count distinct result.
 */
@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Returns the re-aggregated distinct data.
  """)
case class ReCountDistinct(
    child: Expression,
    override val mutableAggBufferOffset: Int = 0,
    override val inputAggBufferOffset: Int = 0)
  extends ImperativeAggregateWithRoaringBitmap with ImplicitCastInputTypes {

  def this(child: Expression) = {
    this(child = child, mutableAggBufferOffset = 0, inputAggBufferOffset = 0)
  }

  override def prettyName: String = "re_count_distinct"

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def children: Seq[Expression] = Seq(child)

  override def nullable: Boolean = false

  override def dataType: DataType = LongType

  override def createAggregationBuffer(): RoaringBitmap = {
    new RoaringBitmap()
  }

  override def update(bitset: RoaringBitmap, input: InternalRow): RoaringBitmap = {
    val v = child.eval(input)
    if (v != null) {
      val bytes = v.asInstanceOf[Array[Byte]]
      val input = deserialize(bytes)
      bitset.or(input)
      bitset
    } else {
      bitset
    }
  }

  override def merge(
      one: RoaringBitmap,
      other: RoaringBitmap): RoaringBitmap = {
    one.or(other)
    one
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)

  override def eval(bitset: RoaringBitmap): Long = {
    bitset.getLongCardinality
  }
}

trait ImperativeAggregateWithRoaringBitmap extends TypedImperativeAggregate[RoaringBitmap] {
  override def serialize(buffer: RoaringBitmap): Array[Byte] = {
    buffer.runOptimize
    val bos = new ByteArrayOutputStream
    val dos = new DataOutputStream(bos)
    buffer.serialize(dos)
    dos.close
    bos.toByteArray
  }

  override def deserialize(bytes: Array[Byte]): RoaringBitmap = {
    val bitset = new RoaringBitmap()
    val bos = new ByteArrayInputStream(bytes)
    val dis = new DataInputStream(bos)
    bitset.deserialize(dis)
    bos.close
    bitset
  }
}

@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Used to get cardinality from pre-computed buffer.
  """)
case class BitSetCardinality(override val child: Expression)
  extends UnaryExpression with ExpectsInputTypes with CodegenFallback {

  override def inputTypes: Seq[DataType] = Seq(BinaryType)

  override def dataType: DataType = LongType

  override def nullSafeEval(input: Any): Long = {
    val data = input.asInstanceOf[Array[Byte]]
    val bitset = new RoaringBitmap()
    val bos = new ByteArrayInputStream(data)
    val dis = new DataInputStream(bos)
    bitset.deserialize(dis)
    bos.close
    bitset.getLongCardinality
  }

  override def prettyName: String = "bitset_cardinality"
}

/**
 * Generate a HyperLogLog which contains all the distinct encoded values, so that its result can be
 * used to do re-aggregation for approx_count_distinct. It's just a placeholder, as
 * PreCountDistinctTransformer rule would rewrite this with HyperLogLogInitSimpleAgg and
 * DictionaryEncode. This function should only be used in CACHE TABLE DDL.
 */
@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Just a placeholder, should be replaced with DictionEncoding and
    HyperLogLogInitSimpleAgg in optimizer.
  """)
case class PreApproxCountDistinct(
    child: Expression,
    relativeSD: Double = 0.05)
  extends ImperativeAggregate with ImplicitCastInputTypes {

  def this(child: Expression) = {
    this(child = child, relativeSD = 0.05)
  }

  def this(child: Expression, relativeSD: Expression) = {
    this(child = child, relativeSD = CacheFunctionUtil.validateDoubleLiteral(relativeSD))
  }

  var dictionaryPath: String = _

  override protected val inputAggBufferOffset: Int = 0

  override protected val mutableAggBufferOffset: Int = 0

  override def prettyName: String = "pre_approx_count_distinct"

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    this

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate = this

  override def update(mutableAggBuffer: InternalRow, inputRow: InternalRow): Unit = {}

  override def merge(mutableAggBuffer: InternalRow, inputAggBuffer: InternalRow): Unit = {}

  override def aggBufferSchema: StructType = StructType.fromAttributes(aggBufferAttributes)

  override def aggBufferAttributes: Seq[AttributeReference] = {Seq.empty[AttributeReference]}

  override def inputAggBufferAttributes: Seq[AttributeReference] = {Seq.empty[AttributeReference]}

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(StringType, BinaryType, NumericType))

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {}

  override def dataType: DataType = BinaryType

  override def children: Seq[Expression] = Seq(child)

  override def initialize(mutableAggBuffer: InternalRow): Unit = {}
}

object CacheFunctionUtil{

  def validateDoubleLiteral(exp: Expression): Double = exp match {
    case Literal(d: Double, DoubleType) => d
    case Literal(dec: Decimal, _) => dec.toDouble
    case _ =>
      throw new AnalysisException("The second argument should be a double literal.")
  }

  def validateIntLiteral(exp: Expression): Int = exp match {
    case Literal(d: Int, IntegerType) => d
    case _ =>
      throw new AnalysisException("The second argument should be a int literal.")
  }
}
