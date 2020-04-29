package com.damavis.spark.resource.parquet

import java.time.LocalDate

import org.apache.spark.sql.{DataFrameReader, SparkSession}

private[resource] case class ParquetReaderParameters(
    path: String,
    sparkReader: DataFrameReader,
    sparkSession: SparkSession,
    from: Option[LocalDate] = None,
    to: Option[LocalDate] = None)
