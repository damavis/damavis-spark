package com.damavis.spark.resource.parquet

import com.damavis.spark.resource.ResourceRW
import org.apache.spark.sql.DataFrame

class ParquetRW extends ResourceRW {
  override def write(data: DataFrame): Unit = ???

  override def read(): DataFrame = ???
}
