package com.damavis.spark.resource.datasource.snowflake

import java.sql.Date

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.WordSpec

class SnowflakeWriterMergerTest extends WordSpec with DataFrameSuiteBase {

  val account = ""
  val user = ""
  val password = ""
  val db = ""
  val warehouse = ""

  "A Snowflake merged write" when {
    "is performed it" should {
      "be an idempotent append" ignore {
        import spark.implicits._
        val data = Seq(
          (1, "user1", Date.valueOf("2020-01-02")),
          (2, "user1", Date.valueOf("2020-01-02")),
          (3, "user1", Date.valueOf("2020-01-02"))
        ).toDF("id", "username", "dt")

        val writer = SnowflakeWriter(account,
                                     user,
                                     password,
                                     warehouse,
                                     db,
                                     "PUBLIC",
                                     "MY_TEST_TABLE")(spark)
        val merger = SnowflakeWriterMerger(writer, Seq("dt"))(spark)
        merger.write(data)
      }
    }
  }
}
