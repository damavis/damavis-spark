package com.damavis.spark.resource

object Format extends Enumeration {
  type Format = Value

  val Avro = Value("avro")
  val Parquet = Value("parquet")
  val Json = Value("json")
  val Csv = Value("csv")
  val Orc = Value("orc")
  val Delta = Value("delta")
}