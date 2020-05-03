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


case class DataFrameQuery(df: DataFrame, as: String, override val joinAs: String,
                          override val idFields: Seq[String] = List("id")) extends Query

object DataFrameQuery {
  def apply(df: DataFrame, as: String): DataFrameQuery = DataFrameQuery(df, as, as)
}


case class DataFrameEventQuery(df: DataFrame, as: String,
                               eventCol: String, startsCol: String, endsCol: String,
                               override val joinAs: String,
                               override val idFields: Seq[String] = List("id")) extends Query with HasEvent {
  override def event: Event = Event(df(eventCol))
  override def starts: Event = Event(df(startsCol))
  override def ends: Event = Event(df(endsCol))
}

object DataFrameEventQuery {
  def apply(df: DataFrame, as: String, eventCol: String): DataFrameEventQuery =
    DataFrameEventQuery(df, as, eventCol, eventCol, eventCol)
  def apply(df: DataFrame, as: String, eventCol: String, startsCol: String, endsCol: String): DataFrameEventQuery =
    DataFrameEventQuery(df, as, eventCol, startsCol, endsCol, as)
}

