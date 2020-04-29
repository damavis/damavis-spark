package com.damavis.spark.resource.parquet

import com.damavis.spark.resource.ResourceWriter
import org.apache.spark.sql.DataFrame

class ParquetWriter(params: ParquetWriterParameters) extends ResourceWriter {
  override def write(data: DataFrame): Unit =
    if (params.columnNames.nonEmpty) {
      data.write
        .mode(params.mode)
        .partitionBy(params.columnNames: _*)
        .parquet(params.path)
    } else {
      data.write.mode(params.mode).parquet(params.path)
    }

}
