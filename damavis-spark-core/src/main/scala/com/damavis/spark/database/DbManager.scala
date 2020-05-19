package com.damavis.spark.database

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object DbManager {
  def useDatabase(name: String, forceCreation: Boolean = false)(
      implicit spark: SparkSession): Database = {
    lazy val logger = LoggerFactory.getLogger(this.getClass)

    val catalog = spark.catalog

    if (!catalog.databaseExists(name))
      if (forceCreation) {
        logger.warn(s"""Database "$name" not found in catalog. Creating it""")
        spark.sql(s"CREATE DATABASE IF NOT EXISTS $name")
        logger.info(s"""Database "$name" created""")
      } else {
        throw new RuntimeException(s"Database $name was not found in catalog")
      }

    catalog.setCurrentDatabase(name)

    val db = catalog.getDatabase(name)

    new Database(db, catalog)
  }
}
