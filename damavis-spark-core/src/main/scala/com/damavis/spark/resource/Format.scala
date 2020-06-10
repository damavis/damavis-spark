package com.damavis.spark.resource

object Format extends Enumeration {
  type Format = Value

  val Avro = Value("avro")
  val Parquet = Value("parquet")
}