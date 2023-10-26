package scalasql.query

import scalasql._
import scalasql.dialects.ReturningDialect
import utest._

import java.time.LocalDate

/**
 * Tests for basic update operations
 */
trait ReturningTests extends ScalaSqlSuite with ReturningDialect {
  override def utestBeforeEach(path: Seq[String]): Unit = checker.reset()
  def tests = Tests {
    test("insert") {
      test("single") - {
        checker(
          query = Buyer.insert
            .values(_.name -> "test buyer", _.dateOfBirth -> LocalDate.parse("2023-09-09"))
            .returning(_.id),
          sql = "INSERT INTO buyer (name, date_of_birth) VALUES (?, ?) RETURNING buyer.id as res",
          value = Seq(4)
        )

        checker(
          query = Buyer.select.filter(_.name === "test buyer"),
          // id=4 comes from auto increment
          value = Seq(Buyer[Id](4, "test buyer", LocalDate.parse("2023-09-09")))
        )
      }

      test("multiple") - {
        checker(
          query = Buyer.insert
            .batched(_.name, _.dateOfBirth)(
              ("test buyer A", LocalDate.parse("2001-04-07")),
              ("test buyer B", LocalDate.parse("2002-05-08")),
              ("test buyer C", LocalDate.parse("2003-06-09"))
            )
            .returning(_.id),
          sql =
            """
            INSERT INTO buyer (name, date_of_birth)
            VALUES
              (?, ?),
              (?, ?),
              (?, ?)
            RETURNING buyer.id as res
          """,
          value = Seq(4, 5, 6)
        )

        checker(
          query = Buyer.select,
          value = Seq(
            Buyer[Id](1, "James Bond", LocalDate.parse("2001-02-03")),
            Buyer[Id](2, "叉烧包", LocalDate.parse("1923-11-12")),
            Buyer[Id](3, "Li Haoyi", LocalDate.parse("1965-08-09")),
            // id=4,5,6 comes from auto increment
            Buyer[Id](4, "test buyer A", LocalDate.parse("2001-04-07")),
            Buyer[Id](5, "test buyer B", LocalDate.parse("2002-05-08")),
            Buyer[Id](6, "test buyer C", LocalDate.parse("2003-06-09"))
          )
        )
      }

      test("select") {
        checker(
          query = Buyer.insert
            .select(
              x => (x.name, x.dateOfBirth),
              Buyer.select.map(x => (x.name, x.dateOfBirth)).filter(_._1 !== "Li Haoyi")
            )
            .returning(_.id),
          sql =
            """
            INSERT INTO buyer (name, date_of_birth)
            SELECT
              buyer0.name as res__0,
              buyer0.date_of_birth as res__1
            FROM buyer buyer0
            WHERE buyer0.name <> ?
            RETURNING buyer.id as res
          """,
          value = Seq(4, 5)
        )

        checker(
          query = Buyer.select,
          value = Seq(
            Buyer[Id](1, "James Bond", LocalDate.parse("2001-02-03")),
            Buyer[Id](2, "叉烧包", LocalDate.parse("1923-11-12")),
            Buyer[Id](3, "Li Haoyi", LocalDate.parse("1965-08-09")),
            // id=4,5 comes from auto increment, 6 is filtered out in the select
            Buyer[Id](4, "James Bond", LocalDate.parse("2001-02-03")),
            Buyer[Id](5, "叉烧包", LocalDate.parse("1923-11-12"))
          )
        )
      }

    }

    test("update") {
      test("single") - {
        checker(
          query =
            Buyer.update
              .filter(_.name === "James Bond")
              .set(_.dateOfBirth -> LocalDate.parse("2019-04-07"))
              .returning(_.id),
          sqls = Seq(
            "UPDATE buyer SET date_of_birth = ? WHERE buyer.name = ? RETURNING buyer.id as res",
            "UPDATE buyer SET buyer.date_of_birth = ? WHERE buyer.name = ? RETURNING buyer.id as res"
          ),
          value = Seq(1)
        )

        checker(
          query = Buyer.select.filter(_.name === "James Bond").map(_.dateOfBirth),
          value = Seq(LocalDate.parse("2019-04-07"))
        )
      }

      test("multiple") - {
        checker(
          query =
            Buyer.update
              .filter(_.name === "James Bond")
              .set(_.dateOfBirth -> LocalDate.parse("2019-04-07"), _.name -> "John Dee")
              .returning(c => (c.id, c.name, c.dateOfBirth)),
          sqls = Seq(
            """
            UPDATE buyer
            SET date_of_birth = ?, name = ? WHERE buyer.name = ?
            RETURNING buyer.id as res__0, buyer.name as res__1, buyer.date_of_birth as res__2
          """,
            """
            UPDATE buyer
            SET buyer.date_of_birth = ?, buyer.name = ? WHERE buyer.name = ?
            RETURNING buyer.id as res__0, buyer.name as res__1, buyer.date_of_birth as res__2
          """
          ),
          value = Seq((1, "John Dee", LocalDate.parse("2019-04-07")))
        )
      }
    }

    test("delete") {
      checker(
        query = Purchase.delete(_.shippingInfoId === 1).returning(_.total),
        sqls = Seq(
          "DELETE FROM purchase WHERE purchase.shipping_info_id = ? RETURNING purchase.total as res",
        ),
        value = Seq(888.0, 900.0, 15.7)
      )

      checker(
        query = Purchase.select,
        value = Seq(
          // id=1,2,3 had shippingInfoId=1 and thus got deleted
          Purchase[Id](id = 4, shippingInfoId = 2, productId = 4, count = 4, total = 493.8),
          Purchase[Id](id = 5, shippingInfoId = 2, productId = 5, count = 10, total = 10000.0),
          Purchase[Id](id = 6, shippingInfoId = 3, productId = 1, count = 5, total = 44.4),
          Purchase[Id](id = 7, shippingInfoId = 3, productId = 6, count = 13, total = 1.3)
        )
      )
    }
  }
}
