package com.damavis.spark.database

import com.damavis.spark.database.exceptions.{
  DatabaseNotFoundException,
  InvalidDatabaseNameException
}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object DbManager {

  private def checkDatabaseName(name: String): Unit = {
    // Enforcing Hive's database naming restrictions (only alphanumeric and "-" characters allowed)
    // Check: https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL

    val normalizedName = name.replace("_", "")
    if (!normalizedName.forall(_.isLetterOrDigit))
      throw new InvalidDatabaseNameException(name)
  }

  def useDatabase(name: String, forceCreation: Boolean = false)(
      implicit spark: SparkSession): Database = {
    lazy val logger = LoggerFactory.getLogger(this.getClass)

    checkDatabaseName(name)

    val catalog = spark.catalog

    if (!catalog.databaseExists(name))
      if (forceCreation) {
        logger.warn(s"""Database "$name" not found in catalog. Creating it""")
        spark.sql(s"CREATE DATABASE IF NOT EXISTS $name")
        logger.info(s"""Database "$name" created""")
      } else {
        throw new DatabaseNotFoundException(name)
      }

    catalog.setCurrentDatabase(name)

    val db = catalog.getDatabase(name)

    new Database(db, catalog)
  }
}
