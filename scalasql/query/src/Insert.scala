package scalasql.query

import scalasql.core.{DialectTypeMappers, Queryable, Expr, WithSqlExpr}

/**
 * A SQL `INSERT` query
 */
trait Insert[VExpr, VCol <: VExpr, VSc, R]
    extends WithSqlExpr[VCol]
    with scalasql.generated.Insert[VExpr, VCol, VSc, R] {
  def table: TableRef
  def qr: Queryable[VCol, R]
  def select[C, R2](
      columns: VExpr => C,
      select: Select[C, R2]
  ): InsertSelect[VExpr, VCol, VSc, C, R, R2]

  def columns(f: (VCol => Column.Assignment[?])*): InsertColumns[VExpr, VCol, VSc, R]
  def values(f: R*): InsertValues[VExpr, VCol, VSc, R]

  def batched[T1](f1: VCol => Column[T1])(items: Expr[T1]*): InsertColumns[VExpr, VCol, VSc, R]

}

object Insert {
  class Impl[VExpr, VCol <: VExpr, VSc, R](val expr: VCol, val table: TableRef)(
      implicit val qr: Queryable.Row[VCol, R],
      dialect: DialectTypeMappers
  ) extends Insert[VExpr, VCol, VSc, R]
      with scalasql.generated.InsertImpl[VExpr, VCol, VSc, R] {

    def newInsertSelect[C, R, R2](
        insert: Insert[VExpr, VCol, VSc, R],
        columns: C,
        select: Select[C, R2]
    ): InsertSelect[VExpr, VCol, VSc, C, R, R2] = { new InsertSelect.Impl(insert, columns, select) }

    def newInsertValues[R](
        insert: Insert[VExpr, VCol, VSc, R],
        columns: Seq[Column[?]],
        valuesLists: Seq[Seq[Expr[?]]]
    )(implicit qr: Queryable[VCol, R]): InsertColumns[VExpr, VCol, VSc, R] = {
      new InsertColumns.Impl(insert, columns, valuesLists)
    }

    def select[C, R2](
        columns: VExpr => C,
        select: Select[C, R2]
    ): InsertSelect[VExpr, VCol, VSc, C, R, R2] = {
      newInsertSelect(this, columns(expr.asInstanceOf[VExpr]), select)
    }

    def columns(f: (VCol => Column.Assignment[?])*): InsertColumns[VExpr, VCol, VSc, R] = {
      val kvs = f.map(_(expr))
      newInsertValues(this, columns = kvs.map(_.column), valuesLists = Seq(kvs.map(_.value)))
    }

    def batched[T1](
        f1: VCol => Column[T1]
    )(items: Expr[T1]*): InsertColumns[VExpr, VCol, VSc, R] = {
      newInsertValues(this, columns = Seq(f1(expr)), valuesLists = items.map(Seq(_)))
    }

    override def values(values: R*): InsertValues[VExpr, VCol, VSc, R] =
      new InsertValues.Impl(this, values, dialect, qr, Nil)
  }
}
