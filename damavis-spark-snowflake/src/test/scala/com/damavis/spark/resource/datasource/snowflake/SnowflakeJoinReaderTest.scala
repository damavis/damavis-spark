package com.damavis.spark.resource.datasource.snowflake

import java.sql.Date

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{FunSuite, WordSpec}

class SnowflakeJoinReaderTest extends WordSpec with DataFrameSuiteBase {

  val account = ""
  val user = ""
  val password = ""
  val db = ""
  val warehouse = ""

  "A Snowflake join read" when {
    "is performed it" should {
      "filter reader with given data" ignore {
        import spark.implicits._
        val data = Seq(
          (Date.valueOf("2020-01-01")),
        ).toDF("dt")

        val reader = SnowflakeReader(account,
                                     user,
                                     password,
                                     warehouse,
                                     db,
                                     "PUBLIC",
                                     Some("MY_TEST_TABLE"))(spark)
        val joinReader = SnowflakeJoinReader(reader, data)(spark)
        assert(joinReader.read().count() == 1)
      }
    }
  }
}
