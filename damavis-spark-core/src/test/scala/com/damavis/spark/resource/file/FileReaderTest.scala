package com.damavis.spark.resource.file

import java.sql.Timestamp
import java.time.{LocalDate, LocalDateTime}

import com.damavis.spark.entities.Log
import com.damavis.spark.resource.Format
import com.damavis.spark.resource.partitioning.DatePartitionFormatter
import com.damavis.spark.testdata._
import com.damavis.spark.utils.{SparkTestBase}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

class FileReaderTest extends SparkTestBase {

  "A FileReader" when {
    "regardless of partitioning" should {
      "read properly parquet data" in {
        val authors = dfFromAuthors(hemingway, wells)
        val path = s"$root/authors1"
        authors.write.parquet(path)

        val reader = FileReaderBuilder(Format.Parquet, path).reader()
        val read = reader.read()

        assertDataFrameEquals(read, authors)
      }

      "read properly avro data" in {
        val authors = dfFromAuthors(hemingway, dickens)
        val path = s"$root/authors2"
        authors.write.format("avro").save(path)

        val reader = FileReaderBuilder(Format.Avro, path).reader()
        val read = reader.read()

        assertDataFrameEquals(read, authors)
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
        val authors =
          dfFromAuthors(hemingway, dickens, hugo, dumas, bradbury)
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

        val expected = spark.createDataFrame(rows, schema)
        assertDataFrameEquals(actorsFromTwo, expected)
      }
    }

    "there is standardHourly date partitioning" should {
      "read only data between the required dates and hour" in {
        val logs = dfFromLogs(
          Log("::1", Timestamp.valueOf("2020-01-01 01:00:00"), "DEBUG", "1"),
          Log("::1", Timestamp.valueOf("2020-01-01 02:00:00"), "DEBUG", "2"),
          Log("::1", Timestamp.valueOf("2020-01-01 03:00:00"), "DEBUG", "3"),
          Log("::1", Timestamp.valueOf("2020-01-01 04:00:00"), "DEBUG", "4"),
          Log("::1", Timestamp.valueOf("2020-01-01 05:00:00"), "DEBUG", "5"),
          Log("::1", Timestamp.valueOf("2020-01-01 06:00:00"), "DEBUG", "6"),
          Log("::1", Timestamp.valueOf("2020-01-01 07:00:00"), "DEBUG", "7"),
          Log("::1", Timestamp.valueOf("2020-01-01 08:00:00"), "DEBUG", "8"),
          Log("::1", Timestamp.valueOf("2020-01-01 09:00:00"), "DEBUG", "9"),
          Log("::1", Timestamp.valueOf("2020-01-01 10:00:00"), "DEBUG", "10"),
          Log("::1", Timestamp.valueOf("2020-01-01 11:00:00"), "DEBUG", "11"),
          Log("::1", Timestamp.valueOf("2020-01-01 12:00:00"), "DEBUG", "12"),
          Log("::1", Timestamp.valueOf("2020-01-01 13:00:00"), "DEBUG", "13"),
          Log("::1", Timestamp.valueOf("2020-01-01 14:00:00"), "DEBUG", "14"),
          Log("::1", Timestamp.valueOf("2020-01-01 15:00:00"), "DEBUG", "15"),
          Log("::1", Timestamp.valueOf("2020-01-01 16:00:00"), "DEBUG", "16"),
          Log("::1", Timestamp.valueOf("2020-01-01 17:00:00"), "DEBUG", "17"),
          Log("::1", Timestamp.valueOf("2020-01-01 18:00:00"), "DEBUG", "18"),
          Log("::1", Timestamp.valueOf("2020-01-01 19:00:00"), "DEBUG", "19"),
          Log("::1", Timestamp.valueOf("2020-01-01 20:00:00"), "DEBUG", "20"),
          Log("::1", Timestamp.valueOf("2020-01-01 21:00:00"), "DEBUG", "21"),
          Log("::1", Timestamp.valueOf("2020-01-01 22:00:00"), "DEBUG", "22"),
          Log("::1", Timestamp.valueOf("2020-01-01 23:00:00"), "DEBUG", "23")
        ).withColumn("year", date_format(col("ts"), "yyyy"))
          .withColumn("month", date_format(col("ts"), "MM"))
          .withColumn("day", date_format(col("ts"), "dd"))
          .withColumn("hour", date_format(col("ts"), "H"))

        val path = s"/$name/logs1"

        logs.write.partitionBy("year", "month", "day", "hour").parquet(path)

        val from = LocalDateTime.parse("2020-01-01T05:00:00")
        val to = LocalDateTime.parse("2020-01-01T18:00:00")
        val data = FileReaderBuilder(Format.Parquet, path)
          .partitioning(DatePartitionFormatter.standardHourly)
          .betweenDates(from, to)
          .reader()
          .read()

        import spark.implicits._
        val actual = data
          .groupBy("ip")
          .agg(min(col("hour")).as("min"), max(col("hour")).as("max"))
        val expected =
          spark.createDataFrame(List(("::1", 5, 18)).toDF.rdd, actual.schema)

        assertDataFrameEquals(actual, expected)
      }
    }

    "there is custom partitioning" should {
      "read only data between the required dates" in {
        //TODO: as the previous test, but using a partitioning scheme different than standard (for instance dt=.../h=)
      }
    }
  }

}
