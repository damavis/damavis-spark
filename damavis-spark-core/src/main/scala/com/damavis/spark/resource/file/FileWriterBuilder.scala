package com.damavis.spark.resource.file

import com.damavis.spark.resource.Format.Format
import com.damavis.spark.resource.{ResourceWriter, WriterBuilder}

object FileWriterBuilder {
  def apply(format: Format, path: String): FileWriterBuilder =
    new FileWriterBuilder(FileWriterParameters(format, path))

  def apply(params: FileWriterParameters): FileWriterBuilder =
    new FileWriterBuilder(params)
}

class FileWriterBuilder(params: FileWriterParameters) extends WriterBuilder {
  override def writer(): ResourceWriter =
    new FileWriter(params)

  def mode(mode: String): FileWriterBuilder =
    new FileWriterBuilder(params.copy(mode = mode))

  def partitionedBy(columnNames: Seq[String]): FileWriterBuilder = {
    if (columnNames.isEmpty)
      throw new RuntimeException(s"columnNames parameter cannot be empty")

    new FileWriterBuilder(params.copy(columnNames = columnNames))
  }
}
