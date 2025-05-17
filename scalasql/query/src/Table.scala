package scalasql.query

import scalasql.core.{DialectTypeMappers, Sc, Queryable, Expr}
import scalasql.core.Context

/**
 * In-code representation of a SQL table, associated with a given `case class` [[V]].
 */
abstract class Table[V[_[_]]]()(implicit name: sourcecode.Name, metadata0: Table.Metadata[V])
    extends TableLike[V[Expr], V[Column], V[Sc]]
    with TableLike.LowPri[V[Expr], V[Column], V[Sc]] {

  override protected def tableMetadata: Table.Metadata[V] = metadata0

  implicit def tableImplicitMetadata: Table.ImplicitMetadata[V] =
    new Table.ImplicitMetadata(metadata0)
}

object Table {
  def metadata[V[_[_]]](t: Table[V]): Table.Metadata[V] = t.tableMetadata
  def ref[V[_[_]]](t: Table[V]): TableRef = TableLike.ref(t)
  def name(t: TableLike.Base): String = TableLike.name(t)
  def labels(t: TableLike.Base): Seq[String] = TableLike.labels(t)
  def columnNameOverride[V[_[_]]](t: TableLike.Base)(s: String): String =
    TableLike.columnNameOverride(t)(s)
  def identifier(t: TableLike.Base)(implicit context: Context): String = TableLike.identifier(t)
  def fullIdentifier(
      t: TableLike.Base
  )(implicit context: Context): String = TableLike.fullIdentifier(t)

  val Internal: TableLike.Internal.type = TableLike.Internal

  case class ImplicitMetadata[V[_[_]]](value: Metadata[V])
  class Metadata[V[_[_]]](
      queryables: (DialectTypeMappers, Int) => Queryable.Row[?, ?],
      walkLabels0: () => Seq[String],
      queryable: (
          () => Seq[String],
          DialectTypeMappers,
          TableLike.Metadata.QueryableProxy
      ) => Queryable[V[Expr], V[Sc]],
      vExpr0: (TableRef, DialectTypeMappers, TableLike.Metadata.QueryableProxy) => V[Column]
  ) extends TableLike.Metadata[V[Expr], V[Column], V[Sc]](
        queryables,
        walkLabels0,
        queryable,
        vExpr0
      )
}

abstract class TableLike[Expr0, Cols0, Row0]()(
    implicit name: sourcecode.Name,
    metadata0: TableLike.Metadata[Expr0, Cols0, Row0]
) extends TableLike.Base
    with TableLike.LowPri[Expr0, Cols0, Row0] {

  protected[scalasql] def tableName = name.value

  protected[scalasql] def schemaName = ""

  protected[scalasql] def escape: Boolean = false

  protected implicit def tableSelf: TableLike[Expr0, Cols0, Row0] = this

  protected def tableMetadata: TableLike.Metadata[Expr0, Cols0, Row0] = metadata0

  implicit def containerQr(implicit dialect: DialectTypeMappers): Queryable.Row[Expr0, Row0] =
    tableMetadata
      .queryable(
        tableMetadata.walkLabels0,
        dialect,
        new TableLike.Metadata.QueryableProxy(tableMetadata.queryables(dialect, _))
      )
      .asInstanceOf[Queryable.Row[Expr0, Row0]]

  protected def tableRef = new TableRef(this)
  protected[scalasql] def tableLabels: Seq[String] = {
    tableMetadata.walkLabels0()
  }
}

trait TableLikeCompanion {}

object TableLike {
  trait LowPri[Expr0, Cols0, Row0] { this: TableLike[Expr0, Cols0, Row0] =>
    implicit def containerQr2(
        implicit dialect: DialectTypeMappers
    ): Queryable.Row[Cols0, Row0] =
      containerQr.asInstanceOf[Queryable.Row[Cols0, Row0]]
  }

  def metadata[Expr0, Cols0, Row0](
      t: TableLike[Expr0, Cols0, Row0]
  ): TableLike.Metadata[Expr0, Cols0, Row0] = t.tableMetadata
  def ref[Expr0, Cols0, Row0](t: TableLike[Expr0, Cols0, Row0]): TableRef = t.tableRef
  def name(t: TableLike.Base): String = t.tableName
  def labels(t: TableLike.Base): Seq[String] = t.tableLabels
  def columnNameOverride[Expr0, Cols0, Row0](t: TableLike.Base)(s: String): String =
    t.tableColumnNameOverride(s)
  def identifier(t: TableLike.Base)(implicit context: Context): String = {
    context.config.tableNameMapper.andThen { str =>
      if (t.escape) {
        context.dialectConfig.escape(str)
      } else {
        str
      }
    }(t.tableName)
  }
  def fullIdentifier(
      t: TableLike.Base
  )(implicit context: Context): String = {
    t.schemaName match {
      case "" => identifier(t)
      case str => s"$str." + identifier(t)
    }
  }
  trait Base {

    /**
     * The name of this table, before processing by [[Config.tableNameMapper]].
     * Can be overriden to configure the table names
     */
    protected[scalasql] def tableName: String
    protected[scalasql] def schemaName: String
    protected[scalasql] def tableLabels: Seq[String]
    protected[scalasql] def escape: Boolean

    /**
     * Customizations to the column names of this table before processing,
     * by [[Config.columnNameMapper]]. Can be overriden to configure the column
     * names on a per-column basis.
     */
    protected[scalasql] def tableColumnNameOverride(s: String): String = identity(s)
  }

  class Metadata[Expr0, Cols0, Row0](
      val queryables: (DialectTypeMappers, Int) => Queryable.Row[?, ?],
      val walkLabels0: () => Seq[String],
      val queryable: (
          () => Seq[String],
          DialectTypeMappers,
          Metadata.QueryableProxy
      ) => Queryable[Expr0, Row0],
      val vExpr0: (TableRef, DialectTypeMappers, Metadata.QueryableProxy) => Cols0
  ) {
    def vExpr(t: TableRef, d: DialectTypeMappers) =
      vExpr0(t, d, new Metadata.QueryableProxy(queryables(d, _)))
  }

  object Metadata extends scalasql.query.TableMacros {
    class QueryableProxy(queryables: Int => Queryable.Row[?, ?]) {
      def apply[T, V](n: Int): Queryable.Row[T, V] = queryables(n).asInstanceOf[Queryable.Row[T, V]]
    }
  }

  object Internal {
    class TableQueryable[Q, R <: scala.Product](
        walkLabels0: () => Seq[String],
        walkExprs0: Q => Seq[Expr[?]],
        construct0: Queryable.ResultSetIterator => R,
        deconstruct0: R => Q = ???
    ) extends Queryable.Row[Q, R] {
      def walkLabels(): Seq[List[String]] = walkLabels0().map(List(_))
      def walkExprs(q: Q): Seq[Expr[?]] = walkExprs0(q)

      def construct(args: Queryable.ResultSetIterator) = construct0(args)

      def deconstruct(r: R): Q = deconstruct0(r)
    }

  }
}
