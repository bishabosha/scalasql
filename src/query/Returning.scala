package scalasql.query

import scalasql.renderer.SqlStr.SqlStringSyntax
import scalasql.renderer.{Context, ExprsToSql, SelectToSql, SqlStr}
import scalasql.{MappedType, Queryable}
import scalasql.utils.OptionPickler

trait Returnable[Q] {
  def expr: Q
  def table: TableRef
  def toSqlQuery(implicit ctx: Context): (SqlStr, Seq[MappedType[_]])
}

trait Returning[Q, R] extends Query[Seq[R]]
object Returning {
  case class Impl[Q, R](returnable: Returnable[_], returning: Q)(implicit
                                                                  val qr: Queryable[Q, R]
  ) extends Returning[Q, R] {
    def valueReader: OptionPickler.Reader[Seq[R]] =
      OptionPickler.SeqLikeReader(qr.valueReader(returning), implicitly)

    def walk() = qr.walk(returning)

    override def singleRow = false

    override def toSqlQuery(implicit ctx0: Context): (SqlStr, Seq[MappedType[_]]) = toSqlQuery0(ctx0)

    def toSqlQuery0(ctx0: Context): (SqlStr, Seq[MappedType[_]]) = {
      implicit val (_, _, _, ctx) = Context.computeContext(ctx0, Nil, Some(returnable.table))

      val (prefix, prevExprs) = returnable.toSqlQuery
      val (flattenedExpr, exprStr) = ExprsToSql.apply0(qr.walk(returning), ctx, sql"")
      val suffix = sql" RETURNING $exprStr"

      (prefix + suffix, flattenedExpr.map(t => Expr.getMappedType(t._2)))
    }
  }

}