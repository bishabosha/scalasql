package scalasql.query

import scalasql.core.Sc
import scala.quoted as q

object TableMacros {

  type Expr[T] = scalasql.core.Expr[T]

  class Wrap[t2, t](val inner: scalasql.core.Queryable.Row[t2, t])
  class Foo(val d: scalasql.core.DialectTypeMappers):

    transparent inline def row[t2, t]: Wrap[t2, t] =
      import d.{given, *}
      Wrap(compiletime.summonInline[scalasql.core.Queryable.Row[t2, t]])

  // class Evaluator(dialect: scalasql.core.DialectTypeMappers):
  //   import dialect.given
  //   transparent inline def row[Tp2, Tp]: scalasql.core.Queryable.Row[Tp2, Tp] =
  //     compiletime.summonInline[scalasql.core.Queryable.Row[Tp2, Tp]]

  // transparent inline def summonRow[Tp2, Tp](dialect: scalasql.core.DialectTypeMappers): scalasql.core.Queryable.Row[Tp2, Tp] = {
  //   Evaluator(dialect).row[Tp2, Tp]
  // }

  def applyImpl[V[_[_]]](using q.Quotes)(using caseClassType: q.Type[V]): q.Expr[Table.Metadata[V]] = {
    // import c.universe._
    import q.quotes.reflect.*

    val vTpe = TypeRepr.of[V]
    val vTparam = vTpe.classSymbol.get.typeMembers.filter(_.isTypeParam).head

    val constructor = vTpe.classSymbol.get.primaryConstructor
    // val ctorTpe = vTpe.memberType(constructor)
    val constructorParameters = constructor.paramSymss.find(ps => ps.nonEmpty && ps.head.isTerm).getOrElse(report.throwError(s"No constructor parameters for class ${vTpe.classSymbol.get}, ${constructor}"))

    def isTypeParamType(param: Symbol) = {
      Ref(param).tpe.widen.typeSymbol != vTparam
    }

    type ColParamMappers = Seq[(q.Expr[_root_.scalasql.query.TableRef], q.Expr[scalasql.core.DialectTypeMappers]) => q.Expr[Any]]

    val columnParams: ColParamMappers = for (param <- constructorParameters) yield {
      val name = param.name

      if (isTypeParamType(param)) {
        (tableRef: q.Expr[_root_.scalasql.query.TableRef], dialect: q.Expr[scalasql.core.DialectTypeMappers]) =>
          val summoner = '{ compiletime.summonInline[scalasql.query.Table.ImplicitMetadata[V]] }
          '{$summoner.value.vExpr($tableRef, $dialect)}
        // q"implicitly[scalasql.Table.ImplicitMetadata[${param.info.typeSymbol}]].value.vExpr($tableRef, dialect)"
      } else {
        Ref(param).tpe.typeArgs.head.asType match {
          case '[t] =>
            (tableRef: q.Expr[_root_.scalasql.query.TableRef], dialect: q.Expr[scalasql.core.DialectTypeMappers]) =>
              val summoner = '{ compiletime.summonInline[scalasql.core.TypeMapper[t]] }
              '{
                val mapper: scalasql.core.TypeMapper[t] = $summoner
                locally:
                  given _root_.scalasql.core.TypeMapper[t] = mapper
                  new _root_.scalasql.query.Column[t]($tableRef, _root_.scalasql.query.Table.columnNameOverride($tableRef.value)(${q.Expr(name)}))
              }
        }
      }
    }

    def subParam(paramInfo: TypeRepr, tpe: TypeRepr) = {
      val res =
      paramInfo match
        case AppliedType(ref, List(t)) =>
          val kind = t.typeSymbol
          if ref.typeSymbol.name == "Nested" then
            ref.appliedTo(tpe) // TODO: how to generalise this?
          else
          if (kind == vTparam) {
            ref.appliedTo(tpe)
          } else {
            tpe.appliedTo(t)
          }
        case _ =>
          report.throwError(s"Could not subParam for ${paramInfo.show}, ${tpe.show}")

      res
    }

    // def subParamTree(paramInfo: TypeRepr, tpe: TypeRepr) = {
    //   val res =
    //   paramInfo match
    //     case AppliedType(ref, List(arg)) =>
    //       (tpe.asType, arg.asType) match
    //         case ('[t2], '[t]) =>
    //            Applied(TypeTree.of[t2], List(TypeTree.of[t]))

    //     case _ =>
    //       paramInfo.asType match
    //         case '[t] =>
    //           TypeTree.of[t]


    //   res
    // }

    val queryables: List[q.Expr[scalasql.core.DialectTypeMappers] => q.Expr[_root_.scalasql.core.Queryable.Row[?, ?]]] =
      for (param <- constructorParameters) yield {
        (dialect: q.Expr[scalasql.core.DialectTypeMappers]) =>
          val tpe = subParam(Ref(param).tpe.widen, TypeRepr.of[Sc])
          val tpe2 = subParam(Ref(param).tpe.widen, TypeRepr.of[TableMacros.Expr])

          // val TM_summonRow = TypeRepr.of[TableMacros.type].classSymbol.get.declaredMethod("summonRow").head


          val result = (tpe.asType, tpe2.asType) match
            case ('[t], '[t2]) => '{
              new Foo(???).row[t2, t]
              ??? : _root_.scalasql.core.Queryable.Row[t2, t]
              // summonRow[tp2, t]($dialect)
            }

          // val result = Ref(TM_summonRow).appliedToTypes(
          //   List(tpe2, tpe)
          // ).appliedTo(dialect.asTerm).asExprOf[_root_.scalasql.core.Queryable.Row[?, ?]]

          report.info(s"queryable for $param: ${result.show}", param.pos.getOrElse(Position.ofMacroExpansion))

          result

          // (tpe.asType, tpe2.asType) match
          //   case ('[t], '[t2]) =>
          //     '{ summonRow[t2, t]($dialect) }
              // q.Expr.summon[scalasql.core.Queryable.Row[t2, t]] match
              //           case None => report.throwError(s"Could not summon Queryable.Row for ${tpe2.show}, ${tpe.show}")
              //           case Some(value) => value
              // '{ compiletime.summonInline[ _root_.scalasql.core.Queryable.Row[t2, t]] }
            //   '{
            //       val d = $dialect
            //       import d.given
            //       compiletime.summonInline[scalasql.core.Queryable.Row[t2, t]]
            //   }

            // val foo = Symbol.requiredMethod("scala.compiletime.summonInline")
            // TypeApply(Ref(foo),
            //   List(TypeTree.of[_root_.scalasql.core.Queryable.Row[t2, t]])
            // ).asExprOf[_root_.scalasql.core.Queryable.Row[?, ?]]
      }

    val constructParams = for ((param, i) <- constructorParameters.zipWithIndex) yield {
      ( queryable: q.Expr[scalasql.query.Table.Metadata.QueryableProxy],
        args: q.Expr[_root_.scalasql.core.Queryable.ResultSetIterator]
      ) =>
        val tpe = subParam(Ref(param).tpe.widen, TypeRepr.of[Sc])
        val tpe2 = subParam(Ref(param).tpe.widen, TypeRepr.of[TableMacros.Expr])
        (tpe.asType, tpe2.asType) match
          case ('[t], '[t2]) =>
            '{ $queryable.apply[t2, t](${q.Expr(i)}).construct($args): _root_.scalasql.core.Sc[t] }
    }

    val deconstructParams = for ((param, i) <- constructorParameters.zipWithIndex) yield {
      ( queryable: q.Expr[scalasql.query.Table.Metadata.QueryableProxy],
        r: q.Expr[V[_root_.scalasql.core.Sc]]
      ) =>
        val tpe = subParam(Ref(param).tpe.widen, TypeRepr.of[Sc])
        val tpe2 = subParam(Ref(param).tpe.widen, TypeRepr.of[TableMacros.Expr])
        (tpe.asType, tpe2.asType) match
          case ('[t], '[t2]) =>
            val select = Select(r.asTerm, param).asExprOf[t]
            '{ $queryable.apply[t2, t](${q.Expr(i)}).deconstruct($select) }
    }

    val flattenLists = for (param <- constructorParameters) yield {
      if (isTypeParamType(param)) {
        '{ compiletime.summonInline[scalasql.query.Table.ImplicitMetadata[V]].value.walkLabels0() }
      } else {
        val name = param.name
        '{_root_.scala.List(${q.Expr(name)}) }
      }
    }

    val flattenExprs = for ((param, i) <- constructorParameters.zipWithIndex) yield {
      ( queryable: q.Expr[scalasql.query.Table.Metadata.QueryableProxy],
        table: q.Expr[V[_root_.scalasql.core.Expr]]
      ) =>
        val tpe = subParam(Ref(param).tpe.widen, TypeRepr.of[Sc])
        val tpe2 = subParam(Ref(param).tpe.widen, TypeRepr.of[TableMacros.Expr])
        (tpe.asType, tpe2.asType) match
          case ('[t], '[t2]) =>
            val select = Select(table.asTerm, param).asExprOf[t2]
            '{ $queryable.apply[t2, t](${q.Expr(i)}).walkExprs($select) }
    }

    val queryableMatch =
      (dialect: q.Expr[scalasql.core.DialectTypeMappers], n: q.Expr[Int]) =>
        val cases = queryables.zipWithIndex.map { case (q, i) => CaseDef(Literal(IntConstant(i)), None, q(dialect).asTerm) }
        Match(n.asTerm, cases).asExprOf[_root_.scalasql.core.Queryable.Row[?, ?]]

    val result: q.Expr[Table.Metadata[V]] = '{

      new _root_.scalasql.query.Table.Metadata[V](
        (dialect, n) => {

          // class Wrap24(d: scalasql.core.DialectTypeMappers) {

          //   import d.{given, *}
          //   def res[T, U] = compiletime.summonInline[scalasql.core.Queryable.Row[T, U]]
          // }
          // new Wrap24(dialect).res[Expr[Int], Int]
          ???
          // new Foo(???).row[Expr[Int], Int].asInstanceOf[_root_.scalasql.core.Queryable.Row[?, ?]]
          // ${
          //   queryableMatch('dialect, 'n)
          // }
        },
        () => ${flattenLists.reduceLeft((l, r) => '{$l ++ $r})},
        (walkLabels0, dialect, queryable) => {
          import dialect._
//
          new _root_.scalasql.query.Table.Internal.TableQueryable[V[scalasql.core.Expr], V[scalasql.core.Sc] & Product](
            walkLabels0,
            (table: V[scalasql.core.Expr]) => ???, //${flattenExprs.reduceLeft((l, r) => q"$l ++ $r")},
            construct0 = (args: _root_.scalasql.core.Queryable.ResultSetIterator) => ??? , //new $caseClassType(..$constructParams),
            deconstruct0 = (r: V[scalasql.core.Sc]) => ??? // new $caseClassType(..$deconstructParams)
          ).asInstanceOf[_root_.scalasql.core.Queryable[V[scalasql.core.Expr], V[scalasql.core.Sc]]]
        },
        (tableRef, dialect, queryable) => {
          // import dialect._

          // new $caseClassType(..$columnParams)
          ??? : V[Column]
        }
      )
    }
    result
  }

}
trait TableMacros {
  implicit transparent inline def initTableMetadata[V[_[_]]]: Table.Metadata[V] = ${ TableMacros.applyImpl[V] }
}
