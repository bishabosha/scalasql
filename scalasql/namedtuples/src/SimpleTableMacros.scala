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
import scala.annotation.tailrec
import scalasql.namedtuples.SimpleTableMacros.BaseRowExpr

object SimpleTableMacros {

  trait Mask[C]:
    type Convert[T[_]] <: AnyNamedTuple | C
    type Base[T[_]] <: AnyNamedTuple

  object Mask:
    import scala.quoted.{Expr as QExpr, *}
    object Impl extends Mask[Any]:
      type Convert[T[_]] = Any
      type Base[T[_]] = AnyNamedTuple

    transparent inline given [C]: Mask[C] = ${ compute[C, NamedTuple.From[C]] }

    def baseErr(using Quotes): QExpr[Nothing] = '{
      compiletime.error(
        "Cannot find a Mask instance for the given type. Please ensure that the type is a case class or a named tuple."
      )
    }

    def compute[C: Type, A <: AnyNamedTuple: Type](using Quotes): QExpr[Mask[C]] =
      computeRoot[C, NamedTuple.Names[A], NamedTuple.DropNames[A]]

    def computeRoot[C: Type, N <: Tuple: Type, V <: Tuple: Type](
        using Quotes
    ): QExpr[Mask[C]] =
      compute1[C, N, V](Nil) match
        case Some('[type res[T[_]] <: AnyNamedTuple; res]) =>
          '{
            Impl.asInstanceOf[
              Mask[
                C
              ] {
                type Convert[T[_]] = T[SimpleTable.Internal.Tombstone.type] match
                  case Expr[?] => res[T]
                  case _ => C
                type Base[T[_]] = res[T]
              }
            ]
          }
        case _ => baseErr

    @tailrec
    def compute1[C: Type, N <: Tuple: Type, V <: Tuple: Type](acc: List[Type[?]])(
        using Quotes
    ): Option[Type[?]] =
      Type.of[V] match
        case '[type vs <: Tuple; (v *: `vs`)] =>
          QExpr.summon[SimpleTable.WrappedMetadata[v]] match
            case Some(_) =>
              computeRec[v] match
                case Some(res) =>
                  compute1[C, N, vs](res :: acc)
                case _ =>
                  None
            case _ =>
              compute1[C, N, vs](Type.of[[T[_]] =>> T[v]] :: acc)
        case '[EmptyTuple] =>
          val tpes =
            acc.reverse.foldRight(Type.of[[T[_]] =>> EmptyTuple].asInstanceOf[Type[?]])(
              (tpe, acc) =>
                ((tpe, acc): @unchecked) match {
                  case ('[type tpe[T[_]]; `tpe`], '[type acc[T[_]] <: Tuple; `acc`]) =>
                    Type.of[[T[_]] =>> tpe[T] *: acc[T]]
                }
            )
          tpes match
            case '[type tpes[T[_]] <: Tuple; `tpes`] =>
              Some(Type.of[[T[_]] =>> NamedTuple.NamedTuple[N, tpes[T]]])
        case _ =>
          None

    def computeRec[V: Type](using Quotes): Option[Type[?]] =
      type vNT = NamedTuple.From[V]
      compute1[V, NamedTuple.Names[vNT], NamedTuple.DropNames[vNT]](Nil)

  def asIArray[T: ClassTag](t: Tuple): IArray[T] = {
    IArray.from(t.productIterator.asInstanceOf[Iterator[T]])
  }
  def asIArrayFlatUnwrapWithIndex[T](
      t: Tuple
  )[U: ClassTag](f: (T, Int) => IterableOnce[U]): IArray[U] = {
    IArray.from(t.productIterator.asInstanceOf[Iterator[T]].zipWithIndex.flatMap(f.tupled))
  }
  def asIArrayUnwrap[T](t: Tuple)[U: ClassTag](f: T => U): IArray[U] = {
    IArray.from(t.productIterator.asInstanceOf[Iterator[T]].map(f))
  }
  def unwrapLabels(t: Tuple, labels: IArray[String]): IndexedSeq[String] = {
    asIArrayFlatUnwrapWithIndex[BaseLabels[?, ?]](t)((l, i) => l.fetch(labels(i))).toIndexedSeq
  }
  def unwrapColumns(t: Tuple): IArray[AnyRef] = {
    asIArrayUnwrap[BaseColumn[?, ?]](t)(_.value)
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

  inline def computeRows[Rows <: Tuple](
      mappers: DialectTypeMappers
  ): IArray[BaseRowExpr[?]] = {
    val rows = mappers match
      case given DialectTypeMappers => compiletime.summonAll[Rows]
    computeRows0(rows)
  }
  inline def computeColumns[Columns <: Tuple](
      mappers: DialectTypeMappers,
      tableRef: TableRef
  ): IArray[AnyRef] = {
    val cols = mappers match
      case given DialectTypeMappers =>
        tableRef match
          case given TableRef => compiletime.summonAll[Columns]
    unwrapColumns(cols)
  }

  def computeRows0(t: Tuple): IArray[BaseRowExpr[?]] = {
    asIArray[BaseRowExpr[?]](t)
  }

  class BaseRowExpr[C](val value: Queryable.Row[?, ?], val valueBase: Queryable.Row[?, ?])
  trait BaseRowExprLowPrio {
    inline given notFound: [T] => (mappers: DialectTypeMappers) => BaseRowExpr[T] =
      import mappers.{*, given}
      BaseRowExpr.shared(compiletime.summonInline[Queryable.Row[Expr[T], Sc[T]]])
  }
  object BaseRowExpr extends BaseRowExprLowPrio {
    def shared[T](row: Queryable.Row[?, ?]): BaseRowExpr[T] = {
      BaseRowExpr(row, row)
    }

    given foundMeta: [C] => (mappers: DialectTypeMappers, m: SimpleTable.WrappedMetadata[C])
      => BaseRowExpr[C] =
      BaseRowExpr(m.metadata.rowExpr(mappers), m.metadata.rowExprBase(mappers))
  }

  class BaseColumn[L, T](val value: AnyRef)
  trait BaseColumnLowPrio {
    inline given notFound: [L <: String, T]
      => (l: ValueOf[L], mappers: DialectTypeMappers, ref: TableRef) => BaseColumn[L, T] =
      import mappers.{*, given}
      val col = new Column[T](
        ref,
        Table.columnNameOverride(ref.value)(l.value)
      )(using compiletime.summonInline[TypeMapper[T]])
      BaseColumn(col)
  }
  object BaseColumn extends BaseColumnLowPrio {
    given foundMeta: [L <: String, T]
      => (mappers: DialectTypeMappers, ref: TableRef, m: SimpleTable.WrappedMetadata[T])
      => BaseColumn[L, T] =
      BaseColumn(m.metadata.metadata0.vExpr(ref, mappers).asInstanceOf[AnyRef])
  }

  class BaseLabels[L, C](val fetch: String => Seq[String])
  trait BaseLabelsLowPrio {
    given notFound: [L <: String, C] => BaseLabels[L, C] =
      new BaseLabels(fetch = label => Seq(label))
  }
  object BaseLabels extends BaseLabelsLowPrio {
    given foundMeta: [L, C] => (m: SimpleTable.WrappedMetadata[C]) => BaseLabels[L, C] =
      new BaseLabels(fetch = _ => m.metadata.metadata0.walkLabels0())
  }

  def setNonNull[T](r: AtomicReference[T | Null])(f: T | Null => T): T = {
    val local = r.get()
    val res =
      if local != null then local
      else r.updateAndGet(f(_))
    res.nn
  }

  def walkAllExprs(
      queryable: Table.Metadata.QueryableProxy
  )(e: AnyNamedTuple): IndexedSeq[Expr[?]] = {
    var i = 0
    val fields = e.asInstanceOf[Tuple].productIterator
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

  def deconstruct(
      queryable: Table.Metadata.QueryableProxy
  )(c: Product): AnyNamedTuple = {
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
    Tuple.fromIArray(buf.result()).asInstanceOf[AnyNamedTuple]
  }

  def labels(t: Tuple): IArray[String] = asIArray(t)

  inline def getMirror[C]: (IArray[String], Mirror.ProductOf[C]) = {
    compiletime.summonFrom { case m: Mirror.ProductOf[C] =>
      (labels(compiletime.constValueTuple[m.MirroredElemLabels]), m)
    }
  }

}

trait SimpleTableMacros {
  inline given initTableMetadata: [C <: Product]
    => (f: SimpleTableMacros.Mask[C]) => SimpleTable.Metadata[C] =
    lazy val mirrorPair = SimpleTableMacros.getMirror[C]
    type Impl = f.Convert
    type ImplBase = f.Base
    type Labels = NamedTuple.Names[NamedTuple.From[C]]
    type Values = NamedTuple.DropNames[NamedTuple.From[C]]
    type Pairs[F[_, _]] = Tuple.Map[
      Tuple.Zip[Labels, Values],
      [X] =>> X match {
        case (a, b) => F[a, b]
      }
    ]
    type FlatLabels = Pairs[SimpleTableMacros.BaseLabels]
    type Columns = Pairs[SimpleTableMacros.BaseColumn]
    type Rows = Tuple.Map[Values, SimpleTableMacros.BaseRowExpr]

    val rowsRef = AtomicReference[IArray[BaseRowExpr[?]] | Null](null)
    val labelsRef = AtomicReference[IndexedSeq[String] | Null](null)

    def queryablesRaw(mappers: DialectTypeMappers, idx: Int): BaseRowExpr[?] =
      SimpleTableMacros.setNonNull(rowsRef)(rows =>
        if rows == null then SimpleTableMacros.computeRows[Rows](mappers)
        else rows.nn
      )(idx)

    def queryables(mappers: DialectTypeMappers, idx: Int): Queryable.Row[?, ?] =
      queryablesRaw(mappers, idx).value

    def queryablesBase(mappers: DialectTypeMappers, idx: Int): Queryable.Row[?, ?] =
      queryablesRaw(mappers, idx).valueBase

    def walkLabels0(labelsArr: => IArray[String])(): Seq[String] =
      SimpleTableMacros.setNonNull(labelsRef)(labels =>
        if labels == null then
          val labelsArr0 = labelsArr
          SimpleTableMacros.unwrapLabels(compiletime.summonAll[FlatLabels], labelsArr0)
        else labels.nn
      )

    def queryable[V[_[_]]](factory: (Mirror.ProductOf[V[Sc]], IArray[AnyRef]) => V[Sc])(
        walkLabels0: () => Seq[String],
        @nowarn("msg=unused") mappers: DialectTypeMappers,
        queryable: Table.Metadata.QueryableProxy
    ): Queryable[V[Expr], V[Sc]] = Table.Internal
      .TableQueryable(
        walkLabels0,
        walkExprs0 = SimpleTableMacros.walkAllExprs(queryable),
        construct0 = args =>
          val (labels, mirror) = mirrorPair
          SimpleTableMacros.construct[V[Sc] & Product](queryable)(
            size = labels.size,
            args = args,
            factory =
              factory(mirror.asInstanceOf[Mirror.ProductOf[V[Sc]]], _).asInstanceOf[Product & V[Sc]]
          )
        ,
        deconstruct0 = values => SimpleTableMacros.deconstruct(queryable)(values)
      )
      .asInstanceOf[Queryable[V[Expr], V[Sc]]]

    def vExpr0[V[_[_]]](
        tableRef: TableRef,
        mappers: DialectTypeMappers,
        @nowarn("msg=unused") queryable: Table.Metadata.QueryableProxy
    ): V[Column] =
      // TODO: we should not cache the columns here because this can be called multiple times,
      //       and each time the captured tableRef should be treated as a fresh value.
      val columns = SimpleTableMacros.computeColumns[Columns](mappers, tableRef)
      Tuple.fromIArray(columns).asInstanceOf[V[Column]]

    val walkLabels = () => walkLabels0(mirrorPair(0))()

    val metadata0 =
      Table.Metadata[Impl](
        queryables,
        walkLabels,
        queryable(SimpleTableMacros.make),
        vExpr0
      )
    val metadataBase =
      Table.Metadata[ImplBase](
        queryablesBase,
        walkLabels,
        queryable((_, data) => Tuple.fromIArray(data).asInstanceOf[ImplBase[Sc]]),
        vExpr0
      )

    SimpleTable.Metadata(f)(metadata0, metadataBase)
}
