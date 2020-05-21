package com.damavis.spark.database

import com.damavis.spark.resource.datasource.enums.Format
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.catalog.{Catalog, Database => SparkDatabase}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import scala.language.postfixOps
import scala.util.Try

class Database(db: SparkDatabase, protected[database] val catalog: Catalog)(
    implicit spark: SparkSession) {

  private lazy val logger = LoggerFactory.getLogger(this.getClass)

  def tableExists(table: String): Boolean = {
    val dbPath = parseAndCheckTableName(table)

    catalog.tableExists(dbPath._2)
  }

  def prepareTable(name: String,
                   format: Format.Format,
                   schema: StructType,
                   repair: Boolean = false): Unit = {
    val dbPath = parseAndCheckTableName(name)
    val actualName = dbPath._2

    if (!tableExists(actualName)) {
      createTable(actualName, format, schema)
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
        (db.name, name)
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

  private def createTable(name: String,
                          format: Format.Format,
                          schema: StructType): Unit = {
    catalog.createTable(name, format.toString, schema, Map[String, String]())
  }

  private def repairTable(name: String): Unit = {
    logger.warn(s"Repairing table: $name")
    spark.sql(s"MSCK REPAIR TABLE $name")

    /*NOTE: In case of using Delta Lake, FSCK function may be also suitable here
   * check: https://docs.databricks.com/spark/latest/spark-sql/language-manual/fsck.html
   */
  }
}
