package scalasql.namedtuples

import scalasql.core.DbApi.Impl
import scalasql.core.DialectTypeMappers
import scalasql.core.Expr
import scalasql.core.Queryable
import scalasql.core.Queryable.ResultSetIterator
import scalasql.core.Sc
import scalasql.dialects.Dialect
import scalasql.dialects.TableOps
import scalasql.namedtuples.SimpleTable.Internal.Tombstone
import scalasql.query.Column
import scalasql.query.Query
import scalasql.query.Table
import scalasql.query.Table.ImplicitMetadata
import sourcecode.Name

import scala.NamedTuple.{NamedTuple, AnyNamedTuple}
import scala.language.implicitConversions

class SimpleTable[C]()(
    using val name0: sourcecode.Name,
    metadata0: => SimpleTable.Metadata[C]
) extends Table.Base
    with SimpleTable.LowPri[C] {

  lazy val metadata: SimpleTable.Metadata[C] = metadata0

  override protected[scalasql] def tableName: String = name0.value

  override protected[scalasql] def schemaName: String = ""

  override protected[scalasql] def tableLabels: Seq[String] = {
    metadata.metadata0.walkLabels0()
  }

  override protected[scalasql] def escape: Boolean = false

  given simpleTableImplicitMetadata: SimpleTable.WrappedMetadata[C] =
    SimpleTable.WrappedMetadata(metadata)

  // implicit def containerQr(
  //     implicit dialect: DialectTypeMappers,
  //     f: SimpleTableMacros.Mask[C]
  // ): Queryable.Row[f.Convert[Expr], C] = metadata
  //   .rowExpr(dialect)
  //   .asInstanceOf[Queryable.Row[f.Convert[Expr], C]]

  implicit def containerQrBase(
      implicit dialect: DialectTypeMappers,
      f: SimpleTableMacros.Mask[C]
  ): Queryable.Row[f.Base[Expr], f.Base[Sc]] = metadata
    .rowExprBase(dialect)
    .asInstanceOf[Queryable.Row[f.Base[Expr], f.Base[Sc]]]
}

object SimpleTable {

  trait LowPrioOps { self: Ops.type =>
    // given NTTableQuery: [N <: Tuple, V <: Tuple, Q <: AnyNamedTuple]
    //   => Tuple.IsMappedBy[Expr][V]
    //   => Q <:< NamedTuple[N, Tuple.InverseMap[V, Expr]]
    //   => Queryable.Row[NamedTuple[N, V], Q] =
    //   ???
  }

  object Ops extends LowPrioOps {

    // given selectDelegate: [T <: AnyNamedTuple, C]
    //   => (table: WrappedMetadata[C])
    //   => (delegate: Queryable.Row[NamedTuple.Map[T, Expr], C])
    //   => Queryable.Row[Query[Seq[T]], Seq[C]] =
    //   ???

    given Syntax: AnyRef {
      extension [T <: AnyNamedTuple](t: T)
        def updates(fs: ((u: TupleUpdater[T]) => u.Patch)*): T =
          val u = tupleUpdater[T]
          val arr = t.asInstanceOf[Tuple].toArray
          fs.foreach: f =>
            val patch = f(u)
            val idx = patch.idx
            arr(idx) = patch.f(arr(idx))
          Tuple.fromIArray(IArray.unsafeFromArray(arr)).asInstanceOf[T]
    }
  }

  trait LowPri[C] { this: SimpleTable[C] =>
    // implicit def containerQr2(
    //     implicit dialect: DialectTypeMappers,
    //     f: SimpleTableMacros.Mask[C]
    // ): Queryable.Row[f.Convert[Column], C] =
    //   containerQr.asInstanceOf[Queryable.Row[f.Convert[Column], C]]
    implicit def containerQr2Base(
        implicit dialect: DialectTypeMappers,
        f: SimpleTableMacros.Mask[C]
    ): Queryable.Row[f.Base[Column], f.Base[Sc]] =
      containerQrBase.asInstanceOf[Queryable.Row[f.Base[Column], f.Base[Sc]]]
  }

  implicit def TableOpsConv[C: {SimpleTableMacros.Mask as f}](
      t: SimpleTable[C]
  )(using dialect: Dialect): TableOps[f.Base] =
    // assume types in f.Result matches
    val tableMetadata = t.metadata.metadataBase.asInstanceOf[Table.Metadata[f.Base]]
    dialect.TableOpsConv(new Table[f.Base](using t.name0, tableMetadata) {
      override protected[scalasql] def tableName: String = t.tableName

      override protected[scalasql] def schemaName: String = t.schemaName

      override protected[scalasql] def tableLabels: Seq[String] = t.tableLabels

      override protected[scalasql] def escape: Boolean = t.escape
    })

  // final class Record[C, Mask <: AnyNamedTuple](data: IArray[AnyRef]) extends Selectable:
  //   type Fields = Mask
  //   def recordIterator: Iterator[Any] = data.iterator.asInstanceOf[Iterator[Any]]
  //   def apply(i: Int): AnyRef = data(i)
  //   def updates(fs: ((u: RecordUpdater[C, Mask]) => u.Patch)*): Record[C, Mask] =
  //     val u = recordUpdater[C, Mask]
  //     val arr = IArray.genericWrapArray(data).toArray
  //     fs.foreach: f =>
  //       val patch = f(u)
  //       val idx = patch.idx
  //       arr(idx) = patch.f(arr(idx))
  //     Record(IArray.unsafeFromArray(arr))

  //   inline def selectDynamic(name: String): AnyRef =
  //     apply(compiletime.constValue[Record.IndexOf[name.type, Record.Names[C], 0]])

  private object TupleUpdaterImpl extends TupleUpdater[AnyNamedTuple]
  def tupleUpdater[Mask <: AnyNamedTuple]: TupleUpdater[Mask] =
    TupleUpdaterImpl.asInstanceOf[TupleUpdater[Mask]]
  sealed trait TupleUpdater[Mask <: AnyNamedTuple] extends Selectable:
    final case class Patch(idx: Int, f: AnyRef => AnyRef)
    type Fields = NamedTuple.Map[
      Mask,
      [X] =>> (X => X) => Patch
    ]
    def apply(i: Int): (AnyRef => AnyRef) => Patch =
      f => Patch(i, f)
    inline def selectDynamic(name: String): (AnyRef => AnyRef) => Patch =
      apply(compiletime.constValue[TupleUtils.IndexOf[name.type, NamedTuple.Names[Mask], 0]])

  object TupleUtils:
    import scala.compiletime.ops.int.*
    type Names[C] = NamedTuple.Names[NamedTuple.From[C]]
    type IndexOf[N, T <: Tuple, Acc <: Int] <: Int = T match {
      case EmptyTuple => -1
      case N *: _ => Acc
      case _ *: t => IndexOf[N, t, S[Acc]]
    }
    type InverseMap[T <: AnyNamedTuple, F[_]] = NamedTuple.Map[
      T,
      [X] =>> X match { case F[t] => t }
    ]

  // object Record:
  //   def fromIArray(data: IArray[AnyRef]): Record[Any, AnyNamedTuple] =
  //     Record(data)

  object Internal {
    case object Tombstone
  }

  opaque type WrappedMetadata[C] = Metadata[C]
  object WrappedMetadata {
    def apply[C](metadata: Metadata[C]): WrappedMetadata[C] = metadata
    extension [C](m: WrappedMetadata[C]) {
      def metadata: Metadata[C] = m
    }
  }
  class Metadata[C](val f: SimpleTableMacros.Mask[C])(
      val metadata0: Table.Metadata[f.Convert],
      val metadataBase: Table.Metadata[f.Base]
  ):
    def rowExpr(
        mappers: DialectTypeMappers
    ): Queryable.Row[f.Convert[Expr], C] =
      metadata0
        .queryable(
          metadata0.walkLabels0,
          mappers,
          new Table.Metadata.QueryableProxy(metadata0.queryables(mappers, _))
        )
        .asInstanceOf[Queryable.Row[f.Convert[Expr], C]]

    def rowExprBase(
        mappers: DialectTypeMappers
    ): Queryable.Row[f.Base[Expr], f.Base[Sc]] =
      metadataBase
        .queryable(
          metadataBase.walkLabels0,
          mappers,
          new Table.Metadata.QueryableProxy(metadataBase.queryables(mappers, _))
        )
        .asInstanceOf[Queryable.Row[f.Base[Expr], f.Base[Sc]]]

  object Metadata extends SimpleTableMacros
}
