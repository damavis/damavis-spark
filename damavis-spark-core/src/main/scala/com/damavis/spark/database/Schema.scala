package com.damavis.spark.database

import com.damavis.spark.resource.datasource.Format
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalog.{Catalog, Database => SparkDatabase}
import org.slf4j.LoggerFactory

import scala.util.Try

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

class Schema(db: SparkDatabase, catalog: Catalog)(
    implicit spark: SparkSession) {

  private lazy val logger = LoggerFactory.getLogger(classOf[Schema])

  def tableExists(table: String): Boolean = {
    val dbPath = parseAndCheckTableName(table)

    catalog.tableExists(dbPath._2)
  }

  def prepareTable(name: String,
                   options: TableOptions,
                   repair: Boolean = false): Unit = {
    val dbPath = parseAndCheckTableName(name)
    val actualName = dbPath._2

    if (!tableExists(actualName)) {
      createTable(actualName, options)
    }

    if (repair) repairTable(actualName)
  }

  def getTable(name: String): Try[Table] = {
    Try {
      val dbPath = parseAndCheckTableName(name)
      val actualName = dbPath._2

      val tableMeta = spark.sql(s"DESCRIBE TABLE FORMATTED $actualName")

      val requiredFields = "Location" :: "Provider" :: "Type" :: Nil

      val fields = tableMeta
        .filter(col("col_name").isInCollection(requiredFields))
        .orderBy(col("col_name") asc)
        .select("data_type")
        .collect()

      val location = fields(0).getString(0)
      val format = Format.withName(fields(1).getString(0).toLowerCase)
      val managed = fields(2).getString(0) == "MANAGED"
      val options = TableOptions(location, format, managed)

      Table(this, actualName, options)
    }
  }

  private def parseAndCheckTableName(name: String): (String, String) = {
    val dbParts =
      if (name.contains(".")) {
        val separatorInd = name.indexOf('.')
        val t = name.splitAt(separatorInd)
        t.copy(_2 = t._2.tail) //Remove "." from name
      } else {
        ("default", name)
      }

    if (dbParts._1 != db.name) {
      val errMsg =
        s"""Table ${dbParts._2} specifies a database different than the one used
           | Database used: ${db.name}
           | Table assumes: ${dbParts._1}""".stripMargin
      logger.error(errMsg)
      throw new RuntimeException(errMsg)
    }

    dbParts
  }

  private def createTable(name: String, options: TableOptions): Unit = {
    val optionsMap = Map("format" -> s"${options.format}")

    catalog.createTable(name, options.path, optionsMap)
  }

  private def repairTable(name: String): Unit = {
    logger.warn(s"Repairing table: $name")
    spark.sql(s"MSCK REPAIR TABLE $name")

    /*NOTE: In case of using Delta Lake, FSCK function may be also suitable here
   * check: https://docs.databricks.com/spark/latest/spark-sql/language-manual/fsck.html
   */
  }
}
