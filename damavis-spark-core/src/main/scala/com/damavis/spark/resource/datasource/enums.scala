package com.damavis.spark.resource.datasource

object OverwritePartitionBehavior extends Enumeration {
  type OverwritePartitionBehavior = Value
  val OVERWRITE_ALL, OVERWRITE_MATCHING = Value
}

object Format extends Enumeration {
  type Format = Value

  val Avro = Value("avro")
  val Parquet = Value("parquet")
}
