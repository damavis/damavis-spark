package com.damavis

import java.time.LocalDate
import java.time.format.DateTimeFormatter._
import java.util.concurrent.atomic.AtomicInteger

import com.damavis.spark.database.{Database, Table}
import com.damavis.spark.entities.Author
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConverters._

package object spark {
  val hemingway: Author =
    Author("Hemingway", 61, LocalDate.parse("1899-07-21"), "USA")
  val wells: Author =
    Author("H.G. Wells", 79, LocalDate.parse("1866-09-21"), "UK")
  val dickens: Author =
    Author("Dickens", 58, LocalDate.parse("1812-02-07"), "UK")
  val bradbury: Author =
    Author("Ray Bradbury", 91, LocalDate.parse("1920-08-22"), "USA")
  val dumas: Author =
    Author("Alexandre Dumas", 68, LocalDate.parse("1802-07-24"), "FR")
  val hugo: Author =
    Author("Victor Hugo", 83, LocalDate.parse("1802-02-26"), "FR")

  val authorsSchema: StructType = StructType(
    StructField("name", StringType, nullable = true) ::
      StructField("deceaseAge", IntegerType, nullable = true) ::
      StructField("birthDate", StringType, nullable = true) ::
      StructField("nationality", StringType, nullable = true) ::
      Nil
  )

  def dfFromAuthors(authors: Author*)(
      implicit session: SparkSession): DataFrame = {
    def rowFromAuthor(author: Author): Row = {
      Row(author.name,
          author.deceaseAge,
          author.birthDate.format(ISO_LOCAL_DATE),
          author.nationality)
    }

    val data = authors.map(rowFromAuthor).asJava

    session.createDataFrame(data, authorsSchema)
  }

  private val tableCount = new AtomicInteger(0)

  def nextTable()(implicit db: Database): Table = {
    val tableName = s"authors${tableCount.addAndGet(1)}"

    val tryTable = db.getTable(tableName)

    assert(tryTable.isSuccess)

    tryTable.get
  }
}
