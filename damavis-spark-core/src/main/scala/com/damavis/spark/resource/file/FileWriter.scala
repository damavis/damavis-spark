package com.damavis.spark.resource.file

import com.damavis.spark.resource.ResourceWriter
import org.apache.spark.sql.DataFrame

class FileWriter(params: FileWriterParameters) extends ResourceWriter {
  override def write(data: DataFrame): Unit = {
    val writer = data.write
    val partitionedWriter =
      if (params.columnNames.nonEmpty)
        writer.partitionBy(params.columnNames: _*)
      else
        writer

    partitionedWriter
      .mode(params.mode)
      .format(params.format.toString)
      .save(params.path)
  }
}
