package scalasql.example

// This file is a simple scratch-pad to demo ideas

import scalasql.simple.{*, given}
import H2Dialect.*

case class Person(name: String, age: Int)
object Person extends SimpleTable(SimpleTable.of[Person]())

case class Country(name: String, continent: Continent)
object Country extends SimpleTable(SimpleTable.of[Country]())

case class Continent(name: String)
object Continent extends SimpleTable(SimpleTable.of[Continent]())

case class City(name: String, population: Int, mayor: Person, country: Country)
object City extends SimpleTable(SimpleTable.of[City]())

def bar(db: DbApi) =
  val m = db.run(
    City.select.filter(_.name === "foo").map(c => (name = c.name, mayor = c.mayor))
  )
  val _: Seq[(name: String, mayor: Person)] = m // demonstrate that mayor maps back to case class.

@main def foo =
  City.select.filter(_.name === "foo").map(c => c.country.continent)
  City.insert.values(City("foo", 42, Person("bar", 23), Country("baz", Continent("qux"))))
  City.insert.columns(_.name := "foo")
  City.insert.batched(_.name, _.population, _.mayor.name)(("foo", 42, "bar"), ("baz", 23, "qux"))

def baz =
  Person.select.filter(_.age > 18).sortBy(_.name).desc
