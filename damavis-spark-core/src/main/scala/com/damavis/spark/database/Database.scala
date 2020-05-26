package com.damavis.spark.database

import com.damavis.spark.resource.datasource.enums.Format
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.catalog.{Catalog, Database => SparkDatabase}
import org.apache.spark.sql.types.{StructField, StructType}
import org.slf4j.LoggerFactory

import scala.language.postfixOps
import scala.util.Try

class Database(db: SparkDatabase, protected[database] val catalog: Catalog)(
    implicit spark: SparkSession) {

  private lazy val logger = LoggerFactory.getLogger(this.getClass)

  def tableExists(table: String): Boolean = {
    val dbPath = parseTableName(table)

    if (dbPath._1 != db.name) false
    else catalog.tableExists(dbPath._2)
  }

  def prepareTable(name: String,
                   format: Format.Format,
                   schema: StructType,
                   partitionCols: Seq[String] = Nil,
                   repair: Boolean = false): Unit = {
    val dbPath = parseAndCheckTableName(name)
    val actualName = dbPath._2

    if (!tableExists(actualName)) {
      createTable(actualName, format, schema, partitionCols: _*)
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

      Table(db.name, actualName, options)
    }
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
      throw new RuntimeException(errMsg)
    }

    dbParts
  }

  private def createTable(name: String,
                          format: Format.Format,
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
                                format: Format.Format,
                                schema: StructType,
                                partitionBy: String*): Unit = {
    val serdeParams = format match {
      case Format.Parquet =>
        ("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
         "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
         "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat")
      case Format.Avro =>
        ("org.apache.hadoop.hive.serde2.avro.AvroSerDe",
         "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat",
         "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat")
    }

    val ddl = s"""CREATE TABLE IF NOT EXISTS $name
                 |(${schema.toDDL})
                 |USING ${format.toString.toUpperCase}
                 |OPTIONS (
                 |  SERDE '${serdeParams._1}',
                 |  INPUTFORMAT '${serdeParams._2}',
                 |  OUTPUTFORMAT '${serdeParams._3}'
                 |)
                 |PARTITIONED BY (${partitionBy.mkString(",")})
                 |""".stripMargin
    spark.sql(ddl)
  }

  private def repairTable(name: String): Unit = {
    logger.warn(s"Repairing table: $name")
    spark.sql(s"MSCK REPAIR TABLE $name")

    /*NOTE: In case of using Delta Lake, FSCK function may be also suitable here
   * check: https://docs.databricks.com/spark/latest/spark-sql/language-manual/fsck.html
   */
  }
}
