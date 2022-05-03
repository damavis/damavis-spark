package com.damavis.spark.resource

import org.apache.spark.sql.DataFrame

class BasicResourceRW(reader: ResourceReader, writer: ResourceWriter) extends ResourceRW {
  override def write(data: DataFrame): Unit = writer.write(data)

  override def read(): DataFrame = reader.read()
}
