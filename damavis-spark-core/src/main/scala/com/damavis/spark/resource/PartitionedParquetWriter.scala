package com.damavis.spark.resource
import org.apache.spark.sql.DataFrame

class PartitionedParquetWriter(path: String,
                               _mode: String,
                               columnNames: Seq[String])
    extends ResourceWriter {
  override def write(data: DataFrame): Unit =
    data.write
      .mode(_mode)
      .partitionBy(columnNames: _*)
      .parquet(path)
}
