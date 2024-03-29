package com.damavis.spark.database

import com.damavis.spark.database.exceptions._
import com.damavis.spark.fs.FileSystem
import com.damavis.spark.resource.Format
import com.damavis.spark.resource.Format.Format
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalog.{Catalog, Database => SparkDatabase}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.delta.{DeltaLog, DeltaTableUtils}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

class Database(db: SparkDatabase,
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

      if (catalog.tableExists(name))
        innerGetTable(name)
      else
        DummyTable(db.name, actualName)
    }
  }

  def getUnmanagedTable(name: String, path: String, format: Format): Try[Table] = {
    Try {
      val dbPath = parseAndCheckTableName(name)
      val actualName = dbPath._2

      if (!fs.pathExists(path)) {
        val msg =
          s"""Requested external table $name from path: "$path".
             |Path not reachable in the provided filesystem""".stripMargin
        throw new TableAccessException(msg)
      }

      if (catalog.tableExists(name)) {
        val tableMeta = innerGetTable(name)
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

    // Table path usually begins with "hdfs:<host>/...", which won't be in the requestedPath
    if (!table.path.endsWith(requestedPath)) {
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

    val tableMeta = getMetadata(name)
    val path = tableMeta.storage.locationUri.get.toURL.toString
    val format = Format.withName(tableMeta.provider.get.toLowerCase)
    val managed = tableMeta.tableType.name.startsWith("MANAGED")

    val columns = extractColumns(name)

    val dbPath = parseAndCheckTableName(name)
    val actualName = dbPath._2
    logger.info(
      s"Inner GET TABLE ${actualName}, path: ${path}, format: ${format.toString}, managed: ${managed.toString}, columns: ${columns
        .toString()}")
    RealTable(this.db.name, actualName, path, format, managed, columns)
  }

  private def getMetadata(name: String): CatalogTable = {

    val db = if (name.contains(".")) Option(name.split("\\.")(0)) else None
    val table = if (name.contains(".")) name.split("\\.")(1) else name

    val catalogTable =
      spark.sessionState.catalog.getTableMetadata(TableIdentifier(table, db))

    logger.info(
      s"Table partitioned by ${catalogTable.partitionColumnNames.mkString("[", ",", "]")}")
    logger.info(catalogTable.schema.treeString)

    // This block of code is necessary because Databricks runtime do not
    // provide DeltaTableUtils.
    try {
      Class.forName("org.apache.spark.sql.delta.DeltaTableUtils")
      if (catalogTable.provider == Option("delta")) {
        DeltaTableUtils.combineWithCatalogMetadata(spark, catalogTable)
      } else {
        catalogTable
      }
    } catch {
      case e: ClassNotFoundException =>
        logger.error("Could not combine catalog and delta meta, Cause: ", e)
        logger.warn("Keeping catalog only data")
        catalogTable
      case e: NoClassDefFoundError =>
        logger.error("Could not combine catalog and delta meta, Cause: ", e)
        logger.warn("Keeping catalog only data")
        catalogTable
      case ue: Throwable =>
        logger.error("Could not combine catalog and delta meta, Unknown Cause: ", ue)
        logger.warn("Keeping catalog only data")
        catalogTable
    }

  }

  private def extractColumns(name: String): Seq[Column] = {

    val metadata = getMetadata(name)
    val partitions = metadata.partitionColumnNames

    val columns = metadata.schema.map(field => {
      Column(
        field.name,
        field.dataType.simpleString,
        partitions.contains(field.name),
        field.nullable)

    })
    columns
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
    if (partitionColumns.isEmpty) {
      rawSQLCreateTable(name, format, schema)
    } else
      rawSQLCreateTable(name, format, schema, partitionColumns: _*)
  }

  private def rawSQLCreateTable(name: String,
                                format: Format,
                                schema: StructType): Unit = {
    val ddl = s"""CREATE TABLE IF NOT EXISTS $name
                 |(${schema.toDDL})
                 |USING ${format.toString.toUpperCase}
                 |""".stripMargin
    logger.info(ddl)
    spark.sql(ddl)
    logger.info(s"Table ${name} created.")
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
    logger.info(ddl)
    spark.sql(ddl)
    logger.info(s"Table ${name} created.")
  }

}
