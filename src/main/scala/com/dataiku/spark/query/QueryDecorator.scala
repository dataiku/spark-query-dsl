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

import org.apache.spark.sql.Column


trait QueryDecorator extends Query {
  val base: Query
  override def leaves: Seq[Query] = base.leaves
  override def nodes: Seq[Query] = List(base)
  override def apply(column: String): Column = base.apply(column)
}

object QueryDecorator {
  def unapply(q: QueryDecorator): Option[Query] = Some(q.base)
}
