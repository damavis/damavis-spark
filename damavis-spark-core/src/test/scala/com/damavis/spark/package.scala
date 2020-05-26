package com.damavis

import com.damavis.spark.entities.Person
import org.apache.spark.sql.{DataFrame, SparkSession}

package object spark {
  val orson: Person = Person("Orson Scott", 68, "USA")
  val wells: Person = Person("Wells", 79, "UK")
  val bradbury: Person = Person("Ray Bradbury", 91, "USA")

  def dfFromAuthors(authors: Person*)(
      implicit session: SparkSession): DataFrame = {
    import session.implicits._

    authors.toDF()
  }
}
