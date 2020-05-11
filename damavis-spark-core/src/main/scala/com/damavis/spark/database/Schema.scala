package com.damavis.spark.database

import com.damavis.spark.resource.datasource.TableOptions
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalog.{Catalog, Database}
import org.slf4j.LoggerFactory

object Schema {
  def useDatabase(name: String, forceCreation: Boolean = false)(
      implicit spark: SparkSession): Schema = {
    lazy val logger = LoggerFactory.getLogger(classOf[Schema])

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

    new Schema(db, catalog)
  }
}

class Schema(db: Database, catalog: Catalog)(implicit spark: SparkSession) {

  private lazy val logger = LoggerFactory.getLogger(classOf[Schema])

  def tableExists(table: String): Boolean =
    catalog.tableExists(table)

  def prepareTable(options: TableOptions, repair: Boolean = false): Unit = {
    if (!tableExists(options.name)) {
      createTable(options)
    }

    if (repair) repairTable(options.name)
  }

  private def createTable(options: TableOptions): Unit = {
    val optionsMap = Map("format" -> s"${options.format}")

    catalog.createTable(options.name, options.path, optionsMap)
  }

  private def repairTable(table: String): Unit = {
    logger.warn(s"Repairing table: $table")
    spark.sql(s"MSCK REPAIR TABLE $table")

    /*NOTE: In case of using Delta Lake, FSCK function may be also suitable here
   * check: https://docs.databricks.com/spark/latest/spark-sql/language-manual/fsck.html
   */
  }
}
