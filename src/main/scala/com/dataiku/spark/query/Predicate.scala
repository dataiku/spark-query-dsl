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
import org.apache.spark.sql.functions.lit


trait Predicate {
  def toColumn: Column
  def toNotColumn: Option[Column] = None

  def &&(p: Predicate): Predicate = AndPredicate(this, p)
  def ||(p: Predicate): Predicate = OrPredicate(this, p)
  def unary_! : Predicate = NotPredicate(this)
}
object Predicate {
  def apply(col: => Column): Predicate = new Predicate() {
    override def toColumn: Column = col
  }
}


case class AndPredicate(ps: Predicate *) extends Predicate {
  override def &&(p: Predicate): AndPredicate = AndPredicate(ps :+ p :_*)
  override def unary_! : OrPredicate = OrPredicate(ps.map(! _) :_*)
  override def toColumn: Column = ps.map(_.toColumn).reduce(_ && _)
}

case class OrPredicate(ps: Predicate *) extends Predicate {
  override def ||(p: Predicate): OrPredicate = OrPredicate(ps :+ p :_*)
  override def unary_! : AndPredicate = AndPredicate(ps.map(! _) :_*)
  override def toColumn: Column = ps.map(_.toColumn).reduce(_ || _)
}

case class NotPredicate(p: Predicate) extends Predicate {
  override def unary_! : Predicate = p
  override def toColumn: Column = p.toNotColumn.getOrElse(! p.toColumn)
  override def toNotColumn: Option[Column] = Some(p.toColumn)
}

case object TruePredicate extends Predicate {
  override def &&(p: Predicate): Predicate = p
  override def ||(p: Predicate): TruePredicate.type = this
  override lazy val unary_! : FalsePredicate.type = FalsePredicate
  override lazy val toColumn: Column = lit(true)
}

case object FalsePredicate extends Predicate {
  override def &&(p: Predicate): FalsePredicate.type = this
  override def ||(p: Predicate): Predicate = p
  override lazy val unary_! : TruePredicate.type = TruePredicate
  override lazy val toColumn: Column = lit(false)
}

case object EmptyPredicate extends Predicate {
  override def &&(p: Predicate): Predicate = p
  override def ||(p: Predicate): Predicate = p
  override lazy val toColumn: Column = lit(true)
}
