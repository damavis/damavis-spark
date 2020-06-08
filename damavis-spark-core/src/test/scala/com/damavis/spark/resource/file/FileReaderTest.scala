package com.damavis.spark.resource.file

import com.damavis.spark._
import com.damavis.spark.resource.Format
import com.damavis.spark.utils.SparkTestSupport

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
      "read only data between the required dates" in {}
    }

    "there is custom partitioning" should {
      "read only data between the required dates" in {}
    }
  }

}
