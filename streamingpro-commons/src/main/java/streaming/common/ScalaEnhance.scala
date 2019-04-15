/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package streaming.common

import scala.annotation.tailrec
import scala.language.experimental.macros
import scala.reflect.macros.blackbox

object ScalaMethodMacros {
  def nameOfImpl(c: blackbox.Context)(x: c.Tree): c.Tree = {
    import c.universe._

    @tailrec def extract(x: c.Tree): String = x match {
      case Ident(TermName(s)) => s
      case Select(_, TermName(s)) => s
      case Function(_, body) => extract(body)
      case Block(_, expr) => extract(expr)
      case Apply(func, _) => extract(func)
    }

    val name = extract(x)
    q"$name"
  }

  def nameOfMemberImpl(c: blackbox.Context)(f: c.Tree): c.Tree = nameOfImpl(c)(f)

  def str(x: Any): String = macro nameOfImpl

  def str[T](f: T => Any): String = macro nameOfMemberImpl
}
