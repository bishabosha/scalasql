package usql.renderer

import usql.query.Expr


/**
 * Represents a SQL query with interpolated `?`s expressions and the associated
 * interpolated values, of type [[Interp]]
 */
case class SqlStr(queryParts: Seq[String],
                  params: Seq[Interp],
                  isCompleteQuery: Boolean) {
  def +(other: SqlStr) = new SqlStr(
    queryParts.init ++ Seq(queryParts.last + other.queryParts.head) ++ other.queryParts.tail,
    params ++ other.params,
    false
  )
}

object SqlStr {
  def opt[T](t: Option[T])(f: T => SqlStr) = t.map(f).getOrElse(usql"")

  def optSeq[T](t: Seq[T])(f: Seq[T] => SqlStr) = if (t.nonEmpty) f(t) else usql""

  def flatten(self: SqlStr) = {
    val finalParts = collection.mutable.Buffer[String]()
    val finalArgs = collection.mutable.Buffer[Interp.Simple]()

    def rec(self: SqlStr): Unit = {
      var boundary = true
      def addFinalPart(s: String) = {
        if (boundary && finalParts.nonEmpty) finalParts(finalParts.length - 1) = finalParts.last + s
        else finalParts.append(s)
      }
      for ((p, a) <- self.queryParts.zip(self.params)) {
        addFinalPart(p)
        boundary = false
        a match {
          case ei: Interp.ExprInterp =>
            rec(ei.e.toSqlExpr(ei.ctx))
            boundary = true
          case si: Interp.SqlStrInterp =>
            rec(si.s)
            boundary = true
          case s: Interp.Simple =>
            finalArgs.append(s)
        }
      }

      addFinalPart(self.queryParts.last)
    }

    rec(self)
    new SqlStr(finalParts.toSeq, finalArgs.toSeq, false)
  }

  implicit class SqlStringSyntax(sc: StringContext) {
    def usql(args: Interp*) = new SqlStr(sc.parts, args, false)
  }

  def join(strs: Seq[SqlStr], sep: SqlStr = usql""): SqlStr = {
    if (strs.isEmpty) usql""
    else strs.reduce(_ + sep + _)
  }

  def raw(s: String) = new SqlStr(Seq(s), Nil, false)
}

sealed trait Interp

object Interp{
  sealed trait Simple extends Interp

  implicit def stringInterp(s: String): Interp = StringInterp(s)
  case class StringInterp(s: String) extends Simple

  implicit def intInterp(i: Int): Interp = IntInterp(i)
  case class IntInterp(i: Int) extends Simple

  implicit def doubleInterp(d: Double): Interp = DoubleInterp(d)
  case class DoubleInterp(d: Double) extends Simple

  implicit def booleanInterp(b: Boolean): Interp = BooleanInterp(b)
  case class BooleanInterp(b: Boolean) extends Simple

  implicit def exprInterp(t: Expr[_])
                         (implicit ctx: Context): Interp = ExprInterp(t, ctx)
  case class ExprInterp(e: Expr[_], ctx: Context) extends Interp

  implicit def sqlStrInterp(s: SqlStr): Interp = SqlStrInterp(s)
  case class SqlStrInterp(s: SqlStr) extends Interp
}