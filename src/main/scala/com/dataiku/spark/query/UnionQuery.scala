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

import org.apache.spark.sql.DataFrame


case class UnionQuery(top: Query, bottom: Query, as: String, byName: Boolean = true) extends Query {
  override val joinAs: String = top.joinAs

  override lazy val df: DataFrame = {
    import compat._ // auto-shim for older spark
    if (byName) top.df.unionByName(bottom.df)
    else        top.df.union(bottom.df)
  }

  override def nodes: Seq[Query] = List(top, bottom)

  override def toString: String = s"""($top & $bottom)"""
}

object UnionQuery {
  def apply(top: Query, bottom: Query): UnionQuery = UnionQuery(top, bottom, top.as)
}

