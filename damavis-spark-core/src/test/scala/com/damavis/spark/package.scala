package com.damavis

import java.util.concurrent.atomic.AtomicInteger

import com.damavis.spark.database.{Database, Table}
import com.damavis.spark.entities._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConverters._

package object spark {
  val hemingway: Author = Author("Hemingway", 61, "USA")
  val wells: Author = Author("H.G. Wells", 79, "UK")
  val dickens: Author = Author("Dickens", 58, "UK")
  val bradbury: Author = Author("Ray Bradbury", 91, "USA")

  val farewell: Book = Book("A Farewell to Arms", 1929, hemingway.name)
  val oldMan: Book = Book("The Old Man and the Sea", 1951, hemingway.name)
  val timeMachine: Book = Book("The Time Machine", 1895, wells.name)
  val moreau: Book = Book("The Island of Doctor Moreau", 1896, wells.name)
  val oliverTwist: Book = Book("Oliver Twist", 1839, dickens.name)
  val expectations: Book = Book("Great expectations", 1861, dickens.name)

  val authorsSchema: StructType = StructType(
    StructField("name", StringType, nullable = true) ::
      StructField("deceaseAge", IntegerType, nullable = true) ::
      StructField("nationality", StringType, nullable = true) ::
      Nil
  )

  def dfFromAuthors(authors: Author*)(
      implicit session: SparkSession): DataFrame = {
    def rowFromAuthor(author: Author): Row = {
      Row(author.name, author.deceaseAge, author.nationality)
    }

    val data = authors.map(rowFromAuthor).asJava
    session.createDataFrame(data, authorsSchema)
  }

  def dfFromBooks(books: Book*)(implicit session: SparkSession): DataFrame = {
    val booksSchema: StructType = StructType(
      StructField("title", StringType, nullable = true) ::
        StructField("publicationYear", IntegerType, nullable = true) ::
        StructField("author", StringType, nullable = false) ::
        Nil
    )

    def rowFromBook(book: Book): Row = {
      Row(book.title, book.publicationYear, book.author)
    }

    val data = books.map(rowFromBook).asJava
    session.createDataFrame(data, booksSchema)
  }

  private val tableCount = new AtomicInteger(0)

  def nextTable()(implicit db: Database): Table = {
    val tableName = s"authors${tableCount.addAndGet(1)}"

    val tryTable = db.getTable(tableName)

    assert(tryTable.isSuccess)

    tryTable.get
  }
}
