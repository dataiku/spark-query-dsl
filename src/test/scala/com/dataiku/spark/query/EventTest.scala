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


class EventTest extends QueryBaseTest {

  test("truth table") {
    val df = spark.sql("SELECT 1 as `ok`")
    val one = DataFrameQuery(df, "one")
    assertQuery(1) {
      one | Event.now
    }
    assertQuery(0) {
      one | ((Event.now > Event.now) || (Event.now < Event.now))
    }
    assertQuery(1) {
      one | ((Event.now >= Event.now) && (Event.now <= Event.now))
    }
    assertQuery(1) {
      one | (Event.now.after(Event.now) && Event.now.before(Event.now))
    }
    assertQuery(1) {
      one | (Event.now > (Event.now - "1 minute"))
    }
    assertQuery(1) {
      one | (Event.now < (Event.now + "1 day"))
    }
  }

  test("filter: events") {
    assertQuery(messages) {
      message | message.happens
    }
    assertQuery(messages) {
      message | message.happens.before(Event.now)
    }
    assertQuery(messages) {
      message | message.happens.before(Event.now - "1 day")
    }
  }

}
