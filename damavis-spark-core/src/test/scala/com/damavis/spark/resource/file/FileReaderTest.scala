package com.damavis.spark.resource.file

import java.time.LocalDate

import org.apache.spark.sql.functions._
import com.damavis.spark._
import com.damavis.spark.resource.Format
import com.damavis.spark.utils.SparkTestSupport
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

class FileReaderTest extends SparkTestSupport {
  "A FileReader" when {
    "regardless of partitioning" should {
      "read properly parquet data" in {
        val authors = dfFromAuthors(hemingway, wells)
        val path = s"$root/authors1"
        authors.write.parquet(path)

        val reader = FileReaderBuilder(Format.Parquet, path).reader()
        val read = reader.read()

        checkDataFramesEqual(read, authors)
      }

      "read properly avro data" in {
        val authors = dfFromAuthors(hemingway, dickens)
        val path = s"$root/authors2"
        authors.write.format("avro").save(path)

        val reader = FileReaderBuilder(Format.Avro, path).reader()
        val read = reader.read()

        checkDataFramesEqual(read, authors)
      }

      "throw exception when trying to read in wrong format" in {
        intercept[Throwable] {
          val authors = dfFromAuthors(hemingway, dickens)
          val path = s"$root/authors3"
          authors.write.format("avro").save(path)

          val reader = FileReaderBuilder(Format.Parquet, path).reader()
          reader.read()
        }
      }
    }

    "there is standard date partitioning" should {
      "read only data between the required dates" in {
        val authors = dfFromAuthors(hemingway, dickens, hugo, dumas, bradbury)
          .withColumn("birthAsDate", to_date(col("birthdate"), "yyyy-MM-dd"))
          .withColumn("year", date_format(col("birthAsDate"), "yyyy"))
          .withColumn("month", date_format(col("birthAsDate"), "MM"))
          .withColumn("day", date_format(col("birthAsDate"), "dd"))
          .drop("birthAsDate")

        val path = s"/$name/authors4"

        authors.write.partitionBy("year", "month", "day").parquet(path)

        val from = LocalDate.parse("1802-01-01")
        val to = LocalDate.parse("1802-12-31")
        val actorsFromTwo = FileReaderBuilder(Format.Parquet, path)
          .betweenDates(from, to)
          .reader()
          .read()

        val rows =
          (Row("Alexandre Dumas", 68, "1802-07-24", "FR", 1802, 7, 24) ::
            Row("Victor Hugo", 83, "1802-02-26", "FR", 1802, 2, 26) ::
            Nil).asJava

        val schema = StructType(
          StructField("name", StringType, nullable = true) ::
            StructField("deceaseAge", IntegerType, nullable = true) ::
            StructField("birthDate", StringType, nullable = true) ::
            StructField("nationality", StringType, nullable = true) ::
            StructField("year", IntegerType, nullable = true) ::
            StructField("month", IntegerType, nullable = true) ::
            StructField("day", IntegerType, nullable = true) ::
            Nil
        )

        val expected = session.createDataFrame(rows, schema)
        checkDataFramesEqual(actorsFromTwo, expected)
      }
    }

    "there is custom partitioning" should {
      "read only data between the required dates" in {
        //TODO: as the previous test, but using a partitioning scheme different than standard (for instance dt=.../h=)
      }
    }
  }

}
