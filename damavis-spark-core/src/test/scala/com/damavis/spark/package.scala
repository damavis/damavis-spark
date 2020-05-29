package com.damavis

import com.damavis.spark.entities.Author
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConverters._

package object spark {
  val hemingway: Author = Author("Hemingway", 61, "USA")
  val wells: Author = Author("H.G. Wells", 79, "UK")
  val dickens: Author = Author("Dickens", 58, "UK")
  val bradbury: Author = Author("Ray Bradbury", 91, "USA")

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
}
