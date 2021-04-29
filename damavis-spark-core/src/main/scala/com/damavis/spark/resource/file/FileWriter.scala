package com.damavis.spark.resource.file

import com.damavis.spark.resource.ResourceWriter
import com.damavis.spark.resource.file.exceptions.FileResourceWriteException
import org.apache.spark.sql.DataFrame

class FileWriter(params: FileWriterParameters) extends ResourceWriter {

  override def write(data: DataFrame): Unit = {
    val writer = data.write
    val partitionedWriter =
      if (params.columnNames.nonEmpty) {
        validatePartitioning(data, params.columnNames)

        writer.partitionBy(params.columnNames: _*)
      } else {
        writer
      }

    partitionedWriter
      .mode(params.mode)
      .format(params.format.toString)
      .save(params.path)
  }

  private def validatePartitioning(data: DataFrame, columnNames: Seq[String]): Unit = {
    val fields = data.schema.fieldNames

    val numberPartitionColumns = columnNames.length
    if (fields.length < numberPartitionColumns) {
      val msg =
        s"""Partitioned DataFrame does not have required partition columns
           |These are: "${columnNames.mkString(",")}"
           |Columns in DataFrame: "${fields.mkString(",")}"
           |""".stripMargin
      throw new FileResourceWriteException(msg)
    }

    val partitionColsInSchema = fields.takeRight(numberPartitionColumns)
    val orderDoesNotMatch = columnNames
      .zip(partitionColsInSchema)
      .exists(t => t._1 != t._2)

    if (orderDoesNotMatch) {
      val msg =
        s"""Partitioned DataFrame does not have partition columns in the required order
           |They should be: "${columnNames.mkString(",")}"
           |Order in DataFrame: "${partitionColsInSchema.mkString(",")}"
           |""".stripMargin

      throw new FileResourceWriteException(msg)
    }
  }

}
