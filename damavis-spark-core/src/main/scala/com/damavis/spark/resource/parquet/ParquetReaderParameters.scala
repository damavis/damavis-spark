package com.damavis.spark.resource.parquet

import java.time.LocalDate

import org.apache.spark.sql.SparkSession

private[resource] case class ParquetReaderParameters(
    path: String,
    sparkSession: SparkSession,
    datePartitioned: Boolean = false,
    from: Option[LocalDate] = None,
    to: Option[LocalDate] = None)
