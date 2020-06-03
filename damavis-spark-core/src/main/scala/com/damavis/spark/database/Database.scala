package com.damavis.spark.database

import com.damavis.spark.database.exceptions._
import com.damavis.spark.fs.FileSystem
import com.damavis.spark.resource.Format
import com.damavis.spark.resource.Format.Format
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalog.{Catalog, Database => SparkDatabase}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import scala.language.postfixOps
import scala.util.Try

class Database(
    db: SparkDatabase,
    fs: FileSystem,
    protected[database] val catalog: Catalog)(implicit spark: SparkSession) {

  private lazy val logger = LoggerFactory.getLogger(this.getClass)

  def name: String = db.name

  def tableExists(table: String): Boolean = {
    val dbPath = parseTableName(table)

    if (dbPath._1 != db.name) false
    else catalog.tableExists(dbPath._2)
  }

  def addTableIfNotExists(table: Table,
                          schema: StructType,
                          format: Format,
                          partitionedBy: Seq[String]): Table = {
    table match {
      case real: RealTable => real
      case _: DummyTable =>
        val name = table.name

        if (!tableExists(name))
          createManagedTable(name, format, schema, partitionedBy: _*)

        innerGetTable(name)
    }
  }

  def getTable(name: String): Try[Table] = {
    Try {
      val actualName = parseAndCheckTableName(name)._2

      if (catalog.tableExists(actualName))
        innerGetTable(actualName)
      else
        DummyTable(db.name, name)
    }
  }

  def getUnmanagedTable(name: String,
                        path: String,
                        format: Format): Try[Table] = {
    Try {
      val dbPath = parseAndCheckTableName(name)
      val actualName = dbPath._2

      if (!fs.pathExists(path)) {
        val msg =
          s"""Requested external table $name from path: "$path".
             |Path not reachable in the provided filesystem""".stripMargin
        throw new TableAccessException(msg)
      }

      if (catalog.tableExists(actualName)) {
        val tableMeta = innerGetTable(actualName)
        validateExternalTable(tableMeta, actualName, path, format)

        tableMeta
      } else {
        catalog.createTable(name, path, format.toString)

        RealTable(db.name, name, path, format, managed = false, Nil)
      }
    }
  }

  private def validateExternalTable(table: Table,
                                    name: String,
                                    requestedPath: String,
                                    requestedFormat: Format): Unit = {
    if (table.managed) {
      val msg =
        s"""Requested external table $name, which is already registered as MANAGED in the catalog"""
      throw new TableAccessException(msg)
    }

    if (table.path != requestedPath) {
      val msg =
        s"""Requested external table $name from path: "$requestedPath".
           |It is already registered in the catalog with a different path.
           |Catalog path: "${table.path}"
           |""".stripMargin
      throw new TableAccessException(msg)
    }

    if (table.format != requestedFormat) {
      val msg =
        s"""Requested external table $name with format: "$requestedFormat".
           |It is already registered in the catalog with format: "${table.format}"
           |""".stripMargin
      throw new TableAccessException(msg)
    }
  }

  private def innerGetTable(name: String): Table = {
    val tableMeta = spark.sql(s"DESCRIBE TABLE FORMATTED $name")

    val requiredFields = "Location" :: "Provider" :: "Type" :: Nil
    val fields = tableMeta
      .filter(col("col_name").isInCollection(requiredFields))
      .orderBy(col("col_name") asc)
      .select("data_type")
      .collect()

    val path = fields(0).getString(0)
    val format = Format.withName(fields(1).getString(0).toLowerCase)
    val managed = fields(2).getString(0) == "MANAGED"

    val columns = extractColumns(name)

    RealTable(db.name, name, path, format, managed, columns)
  }

  private def extractColumns(name: String): Seq[Column] = {
    val columnsDf = catalog
      .listColumns(name)
      .select("name", "dataType", "isPartition", "nullable")
      .collect()

    columnsDf.map(
      row =>
        Column(row.getString(0),
               row.getString(1),
               row.getBoolean(2),
               row.getBoolean(3)))
  }

  private def parseTableName(name: String): (String, String) =
    if (name.contains(".")) {
      val separatorInd = name.indexOf('.')
      val t = name.splitAt(separatorInd)
      t.copy(_2 = t._2.tail) //Remove "." from name
    } else {
      (db.name, name)
    }

  private def parseAndCheckTableName(name: String): (String, String) = {
    val dbParts = parseTableName(name)

    if (dbParts._1 != db.name) {
      val errMsg =
        s"""Table ${dbParts._2} specifies a database different than the one used
           | Database used: ${db.name}
           | Table assumes: ${dbParts._1}""".stripMargin
      logger.error(errMsg)
      throw new TableAccessException(errMsg)
    }

    dbParts
  }

  private def createManagedTable(name: String,
                                 format: Format,
                                 schema: StructType,
                                 partitionColumns: String*): Unit = {
    /*NOTE: As of current Spark version (2.4.5), is not possible to create a partitioned HIVE table using
     * the catalog interface
     * Check spark issue: https://issues.apache.org/jira/browse/SPARK-31001
     * So, if partitions are defined, we use a "traditional" (raw SQL) approach. Hopefully, this won't be necessary in
     * the future
     */
    if (partitionColumns.isEmpty)
      catalog.createTable(name, format.toString, schema, Map[String, String]())
    else
      rawSQLCreateTable(name, format, schema, partitionColumns: _*)
  }

  private def rawSQLCreateTable(name: String,
                                format: Format,
                                schema: StructType,
                                partitionBy: String*): Unit = {
    val ddl = s"""CREATE TABLE IF NOT EXISTS $name
                 |(${schema.toDDL})
                 |USING ${format.toString.toUpperCase}
                 |PARTITIONED BY (${partitionBy.mkString(",")})
                 |""".stripMargin
    spark.sql(ddl)
  }

}
