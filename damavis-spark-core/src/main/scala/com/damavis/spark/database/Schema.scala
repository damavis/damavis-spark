package com.damavis.spark.database

import com.damavis.spark.resource.datasource.TableOptions
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalog.{Catalog, Database}
import org.slf4j.LoggerFactory

object Schema {
  def useDatabase(name: String)(implicit spark: SparkSession): Schema = {
    val catalog = spark.catalog

    if (!catalog.databaseExists(name))
      spark.sql(s"CREATE DATABASE IF NOT EXISTS $name")

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
      if (repair) repairTable(options.name)
    }
  }

  private def createTable(options: TableOptions): Unit = {
    val optionsMap = Map("format" -> s"${options.format}")

    catalog.createTable(options.name, options.path, optionsMap)
  }

  private def repairTable(table: String): Unit = {
    logger.warn(s"Repairing table: $table")
    spark.sql(s"MSCK REPAIR TABLE $table")
  }
}
