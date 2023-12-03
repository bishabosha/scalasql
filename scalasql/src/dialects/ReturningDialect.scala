package scalasql.dialects

import scalasql.core.{  WithExpr, Queryable}
import scalasql.query.{
  InsertReturnable,
  InsertReturning,
  OnConflict,
  Returnable,
  Returning,
}

trait ReturningDialect extends Dialect {
  implicit class InsertReturningConv[Q](r: InsertReturnable[Q]) {
    def returning[Q2, R](f: Q => Q2)(implicit qr: Queryable.Row[Q2, R]): InsertReturning[Q2, R] = {
      new InsertReturning.Impl(r, f(WithExpr.get(r)))
    }
  }

  implicit class ReturningConv[Q](r: Returnable[Q]) {
    def returning[Q2, R](f: Q => Q2)(implicit qr: Queryable.Row[Q2, R]): Returning[Q2, R] = {
      new Returning.Impl(r, f(WithExpr.get(r)))
    }
  }
  implicit class OnConflictUpdateConv[Q, R](r: OnConflict.Update[Q, R]) {
    def returning[Q2, R](f: Q => Q2)(implicit qr: Queryable.Row[Q2, R]): Returning[Q2, R] = {
      new Returning.Impl(r, f(WithExpr.get(r)))
    }
  }
  implicit class OnConflictIgnoreConv[Q, R](r: OnConflict.Ignore[Q, R]) {
    def returning[Q2, R](f: Q => Q2)(implicit qr: Queryable.Row[Q2, R]): Returning[Q2, R] = {
      new Returning.Impl(r, f(WithExpr.get(r)))
    }
  }
}
