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
