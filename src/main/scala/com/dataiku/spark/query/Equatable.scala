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


trait Equatable[A] {
  def eq(a: A): Predicate
  def ===(a: A): Predicate = eq(a)
  def neq(a: A): Predicate = !eq(a)
  def !==(a: A): Predicate = neq(a)
  def =!=(a: A): Predicate = neq(a)
}
