package com.damavis.spark.resource.parquet

import com.damavis.spark.resource.ResourceWriter
import org.apache.spark.sql.DataFrame

object ParquetWriter {
  def apply(path: String): ParquetWriter =
    new ParquetWriter(path, "overwrite")
}

class ParquetWriter(path: String, _mode: String) extends ResourceWriter {
  override def write(data: DataFrame): Unit =
    data.write.mode(_mode).parquet(path)

  def mode(mode: String): ParquetWriter =
    new ParquetWriter(path, mode)

  def partitionedBy(columnNames: Seq[String]): PartitionedParquetWriter =
    new PartitionedParquetWriter(path, _mode, columnNames)
}
