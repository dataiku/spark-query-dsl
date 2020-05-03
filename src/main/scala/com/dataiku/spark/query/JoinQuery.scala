/*
 * Copyright 2020 Adrien Lavoillotte, Dataiku
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataiku.spark.query

import org.apache.spark.sql.{Column, DataFrame}


abstract class JoinQuery extends Query {
  val left: Query
  val right: Query
  val on: Column
  val joinType: String
  val joinOp: String
  val as: String

  lazy val df: DataFrame =
    AliasQuery.getAliasedDataFrame(left).join(AliasQuery.getAliasedDataFrame(right), on, joinType)

  override def leaves: Seq[Query] = left.leaves ++ right.leaves

  override def nodes: Seq[Query] = List(left, right)

  override def toString: String = s"""($left $joinOp $right.on$on)"""

  override def nodeString: String = super.nodeString + s" $on"
}

object JoinQuery {
  def unapply(j: JoinQuery): Option[(Query, Query)] = Some((j.left, j.right))
}


trait CanResolveJoinerTo[A] {
  def apply(left: Query, right: Query, on: Column): A

  // For `a + [joiner]` syntax
  def apply(left: Query, j: Joiner): A = {
    val resolved = j.resolve(left)
    apply(resolved.left, resolved.right, resolved.on)
  }

  // For `a + b`, `a + b.on("col")`, `SomeJoin(a, b)`, `SomeJoin(a, b, "col")` syntax
  def apply(left: Query, right: Query, columns: String *): A = columns match {
    case Nil => apply(left, AutoColumnsJoiner(right))
    case _   => apply(left, ColumnNameJoiner(right, columns :_*))
  }

  // For `SomeJoin(a, b, CommonColumnsJoiner)` syntax
  def apply(left: Query, right: Query, joinerBuilder: Query => Joiner): A = apply(left, joinerBuilder(right))
}


// left + right
case class InnerJoin(left: Query, right: Query, on: Column) extends JoinQuery {
  override val joinType: String = "inner"
  override val joinOp: String = "+"
  override val as = s"${left.as}__w_${right.as}"
}
object InnerJoin extends CanResolveJoinerTo[InnerJoin]


// left % right
case class LeftOuterJoin(left: Query, right: Query, on: Column) extends JoinQuery {
  override val joinType: String = "left_outer"
  override val joinOp: String = "%"
  override val as = s"${left.as}__w_${right.as}"
}
object LeftOuterJoin extends CanResolveJoinerTo[LeftOuterJoin]


// left %> right
case class RightOuterJoin(left: Query, right: Query, on: Column) extends JoinQuery {
  override val joinType: String = "right_outer"
  override val joinOp: String = "%>"
  override val as = s"${left.as}__w_${right.as}"
}
object RightOuterJoin extends CanResolveJoinerTo[RightOuterJoin]


// left %% right
case class FullOuterJoin(left: Query, right: Query, on: Column) extends JoinQuery {
  override val joinType: String = "full_outer"
  override val joinOp: String = "%%"
  override val as = s"${left.as}__w_${right.as}"
}
object FullOuterJoin extends CanResolveJoinerTo[FullOuterJoin]


// left - right
case class LeftAntiJoin(left: Query, right: Query, on: Column) extends JoinQuery {
  override val joinType: String = "left_anti"
  override val joinOp: String = "-"
  override val as = s"${left.as}__wo_${right.as}"
}
object LeftAntiJoin extends CanResolveJoinerTo[LeftAntiJoin]


// left ^ right
case class LeftSemiJoin(left: Query, right: Query, on: Column) extends JoinQuery {
  override val joinType: String = "left_semi"
  override val joinOp: String = "^"
  override val as = s"${left.as}__ex_${right.as}"
  override def leaves: Seq[Query] = left.leaves
}
object LeftSemiJoin extends CanResolveJoinerTo[LeftSemiJoin]


// left * right
case class CrossJoin(left: Query, right: Query) extends JoinQuery {
  override val on: Column = null
  override val joinType: String = "cross"
  override val joinOp: String = "*"
  override val as: String = s"${left.as}__by_${right.as}"
  override lazy val df: DataFrame = {
    import compat._ // auto-shim for older spark
    AliasQuery.getAliasedDataFrame(left).crossJoin(AliasQuery.getAliasedDataFrame(right))
  }
}


