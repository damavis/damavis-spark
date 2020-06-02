package com.damavis.spark.resource.parquet

import com.damavis.spark.resource.{ResourceWriter, WriterBuilder}

object ParquetWriterBuilder {
  def apply(path: String): ParquetWriterBuilder =
    new ParquetWriterBuilder(ParquetWriterParameters(path))

  def apply(params: ParquetWriterParameters): ParquetWriterBuilder =
    new ParquetWriterBuilder(params)
}

class ParquetWriterBuilder(params: ParquetWriterParameters)
    extends WriterBuilder {
  override def writer(): ResourceWriter =
    new ParquetWriter(params)

  def mode(_mode: String): ParquetWriterBuilder =
    new ParquetWriterBuilder(params.copy(mode = _mode))

  def partitionedBy(columnNames: Seq[String]): ParquetWriterBuilder = {
    if (columnNames.isEmpty)
      throw new RuntimeException(s"columnNames parameter cannot be empty")

    new ParquetWriterBuilder(params.copy(columnNames = columnNames))
  }
}
