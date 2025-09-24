/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package edu.uci.ics.amber.operator.sort

import edu.uci.ics.amber.core.executor.OperatorExecutor
import edu.uci.ics.amber.core.tuple.{AttributeType, Schema, Tuple, TupleLike}
import edu.uci.ics.amber.util.JSONUtils.objectMapper

import java.sql.Timestamp
import scala.collection.mutable.ArrayBuffer

class SortOpExec(descString: String) extends OperatorExecutor {

  private val desc: SortOpDesc = objectMapper.readValue(descString, classOf[SortOpDesc])
  private var bufferedTuples: ArrayBuffer[Tuple] = _
  private var preparedCriteria: Seq[SortOpExec.PreparedCriterion] = Seq.empty

  override def open(): Unit = {
    bufferedTuples = ArrayBuffer.empty[Tuple]
    preparedCriteria = Seq.empty
  }

  override def close(): Unit = {
    bufferedTuples.clear()
    preparedCriteria = Seq.empty
  }

  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {
    if (preparedCriteria.isEmpty) {
      preparedCriteria = SortOpExec.prepareCriteria(tuple.getSchema, desc.attributes)
    }
    bufferedTuples.append(tuple)
    Iterator.empty
  }

  override def onFinish(port: Int): Iterator[TupleLike] = {
    if (bufferedTuples.isEmpty || preparedCriteria.isEmpty) {
      val result = bufferedTuples.iterator
      bufferedTuples = ArrayBuffer.empty[Tuple]
      return result
    }

    implicit val tupleOrdering: Ordering[Tuple] = SortOpExec.tupleOrdering(preparedCriteria)
    val sorted = bufferedTuples.toSeq.sorted
    bufferedTuples.clear()
    sorted.iterator
  }
}

object SortOpExec {

  private case class PreparedCriterion(index: Int, compare: (Tuple, Tuple) => Int)

  private[sort] def prepareCriteria(
      schema: Schema,
      attributes: List[SortCriteriaUnit]
  ): Seq[PreparedCriterion] = {
    attributes.map { criterion =>
      val attributeName = criterion.attributeName
      require(
        schema.containsAttribute(attributeName),
        s"Attribute '$attributeName' does not exist in the input schema"
      )
      val attributeIndex = schema.getIndex(attributeName)
      val attributeType = schema.getAttribute(attributeName).getType
      val compareFunc = valueComparator(attributeType, criterion.sortPreference == SortPreference.ASC)
      PreparedCriterion(
        attributeIndex,
        (left, right) => {
          val leftValue = left.getField[Any](attributeIndex)
          val rightValue = right.getField[Any](attributeIndex)
          compareFunc(leftValue, rightValue)
        }
      )
    }
  }

  private[sort] def tupleOrdering(criteria: Seq[PreparedCriterion]): Ordering[Tuple] =
    new Ordering[Tuple] {
      override def compare(x: Tuple, y: Tuple): Int = {
        var idx = 0
        while (idx < criteria.length) {
          val result = criteria(idx).compare(x, y)
          if (result != 0) {
            return result
          }
          idx += 1
        }
        0
      }
    }

  private def valueComparator(
      attributeType: AttributeType,
      ascending: Boolean
  ): (Any, Any) => Int = {
    val typedComparator: (Any, Any) => Int = attributeType match {
      case AttributeType.INTEGER =>
        (left, right) => Ordering.Int.compare(asNumber(left).intValue(), asNumber(right).intValue())
      case AttributeType.LONG =>
        (left, right) => Ordering.Long.compare(asNumber(left).longValue(), asNumber(right).longValue())
      case AttributeType.DOUBLE =>
        (left, right) => Ordering.Double.TotalOrdering.compare(
            asNumber(left).doubleValue(),
            asNumber(right).doubleValue()
          )
      case AttributeType.BOOLEAN =>
        (left, right) => Ordering.Boolean.compare(asBoolean(left), asBoolean(right))
      case AttributeType.TIMESTAMP =>
        (left, right) => Ordering.Long.compare(asTimestamp(left).getTime, asTimestamp(right).getTime)
      case AttributeType.STRING =>
        (left, right) => Ordering.String.compare(asString(left), asString(right))
      case AttributeType.BINARY =>
        (left, right) => compareBinary(asBytes(left), asBytes(right))
      case AttributeType.ANY =>
        (left, right) => Ordering.String.compare(asString(left), asString(right))
    }

    (left, right) => {
      (Option(left), Option(right)) match {
        case (None, None) => 0
        case (None, Some(_)) => 1
        case (Some(_), None) => -1
        case (Some(lv), Some(rv)) =>
          val comparison = typedComparator(lv, rv)
          if (ascending) comparison else -comparison
      }
    }
  }

  private def asNumber(value: Any): Number = value match {
    case number: Number => number
    case other => throw new IllegalArgumentException(s"Expected numeric value but found: $other")
  }

  private def asBoolean(value: Any): Boolean = value match {
    case bool: java.lang.Boolean => bool
    case other => throw new IllegalArgumentException(s"Expected boolean value but found: $other")
  }

  private def asTimestamp(value: Any): Timestamp = value match {
    case ts: Timestamp => ts
    case other => throw new IllegalArgumentException(s"Expected timestamp value but found: $other")
  }

  private def asString(value: Any): String = value match {
    case null => null
    case str: String => str
    case other => other.toString
  }

  private def asBytes(value: Any): Array[Byte] = value match {
    case bytes: Array[Byte] => bytes
    case other => throw new IllegalArgumentException(s"Expected binary value but found: $other")
  }

  private def compareBinary(left: Array[Byte], right: Array[Byte]): Int = {
    if (left eq right) {
      return 0
    }
    val minLength = Math.min(left.length, right.length)
    var idx = 0
    while (idx < minLength) {
      val cmp = java.lang.Byte.compare(left(idx), right(idx))
      if (cmp != 0) {
        return cmp
      }
      idx += 1
    }
    java.lang.Integer.compare(left.length, right.length)
  }
}
