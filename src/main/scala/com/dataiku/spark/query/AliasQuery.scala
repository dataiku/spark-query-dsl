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


// Notably allows filtering on both sides of self join:
// p = q.alias("parent")
// joined = p + q.on("id" -> "parent_id")
// joined | p("id") < q("id")
case class AliasQuery(base: Query, as: String) extends QueryDecorator {
  override lazy val df: DataFrame = base.df.as(as)

  import org.apache.spark.sql.functions.col
  override def apply(column: String): Column =
    if (df.columns.count(_ == column) == 1) col(as + "." + column)
    else                                    super.apply(column)

  override def toString: String = {
    val b = base.toString
    if (b == as) b
    else s"""$b.as("$as")"""
  }
}

object AliasQuery {
  def apply(q: Query): AliasQuery = apply(q.as)(q)
  def apply(as: String)(q: Query): AliasQuery = q match {
    case AliasQuery(base, _) => AliasQuery(base, as)
    case _                   => AliasQuery(q, as)
  }

  def getAliasedDataFrame(q: Query): DataFrame = q match {
    case AliasQuery(_, _) => q.df
    case _                => q.df.as(q.as)
  }
}
