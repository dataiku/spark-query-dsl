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


case class FilterQuery(base: Query, filter: Column, as: String) extends QueryDecorator {
  lazy val df: DataFrame = base.df.where(filter)

  override def |(c: Column): FilterQuery = FilterQuery(base, filter && c, as)

  override def toString: String = s"""($base | $filter)"""

  override def nodeString: String = super.nodeString + s" $filter"
}

