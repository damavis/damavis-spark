package com.damavis.spark.database

import com.damavis.spark.database.exceptions.{
  DatabaseNotFoundException,
  InvalidDatabaseNameException
}
import com.damavis.spark.utils.SparkTestSupport
import org.apache.spark.sql.functions._

import scala.util.Try

class DbManagerTest extends SparkTestSupport {
  "A DbManager" when {
    "accessed" should {
      "throw exception if asked for a database with invalid name" in {
        val thrown = intercept[Throwable] {
          DbManager.useDatabase("invalid-database-name")
        }

        assert(thrown.getClass == classOf[InvalidDatabaseNameException])
      }
    }

    "database does not exist in catalog" should {
      "throw exception" in {
        val thrown = intercept[Throwable] {
          DbManager.useDatabase("non_existing")
        }

        assert(thrown.getClass == classOf[DatabaseNotFoundException])
      }

      "create it if told so" in {
        DbManager.useDatabase("newly_created", forceCreation = true)

        assert(spark.catalog.currentDatabase == "newly_created")
      }
    }

    "database does exist in catalog" should {
      "recover it" in {
        DbManager.useDatabase("already_existing", forceCreation = true)

        val df = spark.catalog
          .listDatabases()
          .filter(col("name") === lit("already_existing"))

        assert(df.count == 1)

        val tryDb = Try {
          DbManager.useDatabase("already_existing")
        }

        assert(tryDb.isSuccess)
      }
    }
  }

}
