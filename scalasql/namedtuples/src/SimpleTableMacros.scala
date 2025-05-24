package scalasql.namedtuples

import scalasql.query.Table
import scalasql.core.Queryable
import scalasql.core.DialectTypeMappers
import scalasql.core.Expr
import scalasql.core.Sc
import scalasql.query.TableRef
import scalasql.query.Column
import scala.deriving.Mirror
import java.util.concurrent.atomic.AtomicReference
import scala.reflect.ClassTag
import scala.NamedTuple.AnyNamedTuple
import java.util.function.UnaryOperator
import scala.annotation.nowarn
import scalasql.namedtuples.SimpleTableMacros.BaseLabels
import scalasql.core.TypeMapper
import scala.annotation.unused
import scala.annotation.implicitNotFound

object SimpleTableMacros {
  def asIArray[T: ClassTag](t: Tuple): IArray[T] = {
    IArray.from(t.productIterator.asInstanceOf[Iterator[T]])
  }
  def asIArrayFlatUnwrapWithIndex[T](
      t: Tuple
  )[U: ClassTag](f: (T, Int) => IterableOnce[U]): IArray[U] = {
    IArray.from(t.productIterator.asInstanceOf[Iterator[T]].zipWithIndex.flatMap(f.tupled))
  }
  def unwrapLabels(t: Tuple, labels: IArray[String]): IndexedSeq[String] = {
    asIArrayFlatUnwrapWithIndex[BaseLabels[Any, Any]](t)((l, i) => l(labels(i))).toIndexedSeq
  }
  def unwrapColumns(t: Tuple): IArray[(DialectTypeMappers, TableRef) => AnyRef] = {
    asIArray[ContraRefMapper[BaseColumn[Any, Any]]](t)
  }
  def unwrapRows(t: Tuple): IArray[DialectTypeMappers => Queryable.Row[?, ?]] = {
    asIArray[ContraMapper[SimpleTableMacros.BaseRowExpr[Any]]](t)
  }
  def make[C](m: Mirror.ProductOf[C], data: IArray[AnyRef]): C = {
    class ArrayProduct extends Product {
      override def canEqual(that: Any): Boolean = false
      override def productElement(n: Int): Any = data(n)
      override def productIterator: Iterator[Any] = data.iterator
      override def productPrefix: String = "ArrayProduct"
      override def productArity: Int = data.length
    }
    m.fromProduct(ArrayProduct())
  }

  opaque type ContraMapper[T] = DialectTypeMappers => T

  object ContraMapper {
    inline given [T]: ContraMapper[T] =
      case mappers =>
        import mappers.given
        compiletime.summonInline[T]
  }

  opaque type ContraRefMapper[T] = (DialectTypeMappers, TableRef) => T

  object ContraRefMapper {
    @nowarn("msg=inline given alias")
    inline given [T]: ContraRefMapper[T] =
      (mappers, tableRef) =>
        tableRef match
          case given TableRef =>
            import mappers.given
            compiletime.summonInline[T]
  }

  opaque type IsTableEv[T, +Out <: Option[T]] = Unit
  object IsTableEv {
    given summonDelegate[T, Tables](
        using @unused m: SimpleTable.GivenMetadata[T, Tables]
    ): IsTableEv[T, Some[T]] = ()

    given summonBasic: [T]
      => (@unused ev: scala.util.NotGiven[SimpleTable.AnyTableMetadata[T]])
      => IsTableEv[T, None.type] = ()
  }

  /** A sequence of n `Queryable.Row[Q, R]` instances, where `X` corresponds to all the `Q` and `Y` to all the `R` */
  opaque type IsTableEvs[X <: Tuple, +Y] = Unit

  object IsTableEvs {
    type Of[C, +Tables] = IsTableEvs[NamedTuple.DropNames[NamedTuple.From[C]], Tables]

    case object Empty
    type Empty = IsTableEvs.Empty.type
    type Combine[R, T <: Option[Any]] = T match {
      case Some[t] =>
        R match
          case Empty => t
          case _ => R | t
      case _ => R
    }
    type CombineAny[R, T] = T match {
      case Empty => R
      case _ => Combine[R, Some[T]]
    }

    type FieldsOf[C] = NamedTuple.DropNames[NamedTuple.From[C]]

    given concatRows: [Q, Nested, Qs <: Tuple, Rs]
      => (x: IsTableEv[Q, Some[Q]])
      => (nested: IsTableEvs[FieldsOf[Q], Nested])
      => (xs: IsTableEvs[Qs, Rs])
      => IsTableEvs[Q *: Qs, CombineAny[Combine[Rs, Some[Q]], Nested]] = ()

    given concatRowsNope: [Q, Qs <: Tuple, Rs]
      => (x: IsTableEv[Q, None.type])
      => (xs: IsTableEvs[Qs, Rs])
      => IsTableEvs[Q *: Qs, Rs] = ()

    given emptyRows: IsTableEvs[EmptyTuple, IsTableEvs.Empty] = ()
  }

  opaque type BaseRowExpr[T] = Queryable.Row[?, ?]
  object BaseRowExpr {
    given summonDelegate[T, Tables](
        using @unused m: SimpleTable.GivenMetadata[T, Tables]
    )(
        using delegate: Queryable.Row[
          SimpleTable.MapOver[T, Expr, Tables],
          SimpleTable.MapOver[T, Sc, Tables]
        ]
    ): BaseRowExpr[T] = delegate
    given summonBasic[T](using @unused ev: scala.util.NotGiven[SimpleTable.AnyTableMetadata[T]])(
        using delegate: Queryable.Row[Expr[T], Sc[T]]
    ): BaseRowExpr[T] = delegate
  }

  opaque type BaseColumn[L, T] = AnyRef
  trait BaseColumnLowPrio {
    given notFound: [L <: String, T]
      => (l: ValueOf[L], ref: TableRef, mapper: TypeMapper[T])
      => BaseColumn[L, T] =
      val col = new Column[T](
        ref,
        Table.columnNameOverride(ref.value)(l.value)
      )
      col
  }
  object BaseColumn extends BaseColumnLowPrio {
    given foundMeta: [L <: String, T, Tables]
      => (mappers: DialectTypeMappers, ref: TableRef, m: SimpleTable.GivenMetadata[T, Tables])
      => BaseColumn[L, T] =
      m.metadata.vExpr(ref, mappers).asInstanceOf[AnyRef]
  }

  opaque type BaseLabels[L, C] = String => Seq[String]
  trait BaseLabelsLowPrio {
    given notFound: [L <: String, C] => BaseLabels[L, C] =
      label => Seq(label)
  }
  object BaseLabels extends BaseLabelsLowPrio {
    given foundMeta
        : [L, C, Tables] => (m: SimpleTable.GivenMetadata[C, Tables]) => BaseLabels[L, C] =
      _ => m.metadata.walkLabels0()
  }

  def setNonNull[T](r: AtomicReference[T | Null])(f: => T): T = {
    val local = r.get()
    val res =
      if local != null then local
      else
        r.updateAndGet(t =>
          if t == null then f
          else t
        )
    res.nn
  }

  def walkAllExprs(
      queryable: Table.Metadata.QueryableProxy
  )(e: SimpleTable.Record[?, ?, ?]): IndexedSeq[Expr[?]] = {
    var i = 0
    val fields = e.productIterator
    val buf = IndexedSeq.newBuilder[Seq[Expr[?]]]
    while fields.hasNext do
      type T
      type Field
      val field = fields.next().asInstanceOf[Field]
      val row = queryable[Field, T](i)
      buf += row.walkExprs(field)
      i += 1
    buf.result().flatten
  }

  def construct[C](
      queryable: Table.Metadata.QueryableProxy
  )(size: Int, args: Queryable.ResultSetIterator, factory: IArray[AnyRef] => C): C = {
    var i = 0
    val buf = IArray.newBuilder[AnyRef]
    while i < size do
      type T
      type Field
      val row = queryable[Field, T](i)
      buf += row.construct(args).asInstanceOf[AnyRef]
      i += 1
    factory(buf.result())
  }

  def deconstruct[R <: SimpleTable.Record[?, ?, ?]](
      queryable: Table.Metadata.QueryableProxy
  )(c: Product): R = {
    var i = 0
    val buf = IArray.newBuilder[AnyRef]
    val fields = c.productIterator
    while fields.hasNext do
      type T
      type Field
      val field = fields.next().asInstanceOf[T]
      val row = queryable[Field, T](i)
      buf += row.deconstruct(field).asInstanceOf[AnyRef]
      i += 1
    SimpleTable.Record.fromIArray(buf.result()).asInstanceOf[R]
  }

  def labels(t: Tuple): IArray[String] = asIArray(t)

  inline def getMirror[C]: (Tuple, Mirror.ProductOf[C]) = {
    compiletime.summonFrom { case m: Mirror.ProductOf[C] =>
      (compiletime.constValueTuple[m.MirroredElemLabels], m)
    }
  }

}

trait SimpleTableMacros {
  class SimpleTableState[C <: Product](
      mirrorPair0: => (Tuple, Mirror.ProductOf[C]),
      rowsRef0: => Tuple,
      colsRef0: => Tuple,
      labelsRef0: => Tuple
  ):
    private lazy val mirrorPair =
      val (names0, mirror0) = mirrorPair0
      (
        SimpleTableMacros.labels(names0),
        mirror0
      )
    def labels: IArray[String] = mirrorPair(0)
    def mirror: Mirror.ProductOf[C] = mirrorPair(1)
    lazy val rowsRef: IArray[DialectTypeMappers => Queryable.Row[?, ?]] =
      SimpleTableMacros.unwrapRows(rowsRef0)
    lazy val colsRef: IArray[(DialectTypeMappers, TableRef) => AnyRef] =
      SimpleTableMacros.unwrapColumns(colsRef0)
    lazy val labelsRef: IndexedSeq[String] =
      SimpleTableMacros.unwrapLabels(labelsRef0, labels)

  inline given initTableMetadata[C <: Product, Tables]
      : Table.Metadata[[T[_]] =>> SimpleTable.MapOver[C, T, Tables]] =
    type Impl[T[_]] = SimpleTable.MapOver[C, T, Tables]
    type Labels = NamedTuple.Names[NamedTuple.From[C]]
    type Values = NamedTuple.DropNames[NamedTuple.From[C]]
    type Pairs[F[_, _]] = Tuple.Map[
      Tuple.Zip[Labels, Values],
      [X] =>> X match {
        case (a, b) => F[a, b]
      }
    ]
    type FlatLabels = Pairs[SimpleTableMacros.BaseLabels]
    type Columns = Pairs[[L,
    T] =>> SimpleTableMacros.ContraRefMapper[SimpleTableMacros.BaseColumn[L, T]]]
    type Rows = Tuple.Map[
      Values,
      [T] =>> SimpleTableMacros.ContraMapper[SimpleTableMacros.BaseRowExpr[T]]
    ]

    val state = new SimpleTableState[C](
      mirrorPair0 = SimpleTableMacros.getMirror[C],
      rowsRef0 = compiletime.summonAll[Rows],
      colsRef0 = compiletime.summonAll[Columns],
      labelsRef0 = compiletime.summonAll[FlatLabels]
    )

    def queryables(mappers: DialectTypeMappers, idx: Int): Queryable.Row[?, ?] =
      state.rowsRef(idx)(mappers)

    def walkLabels0(): Seq[String] = state.labelsRef

    def queryable(
        walkLabels0: () => Seq[String],
        @nowarn("msg=unused") mappers: DialectTypeMappers,
        queryable: Table.Metadata.QueryableProxy
    ): Queryable[Impl[Expr], Impl[Sc]] = Table.Internal.TableQueryable(
      walkLabels0,
      walkExprs0 = SimpleTableMacros.walkAllExprs(queryable),
      construct0 = args =>
        SimpleTableMacros.construct(queryable)(
          size = state.labels.size,
          args = args,
          factory = SimpleTableMacros.make(state.mirror, _)
        ),
      deconstruct0 = values => SimpleTableMacros.deconstruct[Impl[Expr]](queryable)(values)
    )

    def vExpr0(
        tableRef: TableRef,
        mappers: DialectTypeMappers,
        @nowarn("msg=unused") queryable: Table.Metadata.QueryableProxy
    ): Impl[Column] =
      val columns = state.colsRef.map(_(mappers, tableRef))
      SimpleTable.Record.fromIArray(columns).asInstanceOf[Impl[Column]]

    Table.Metadata[Impl](queryables, walkLabels0, queryable, vExpr0)
}
