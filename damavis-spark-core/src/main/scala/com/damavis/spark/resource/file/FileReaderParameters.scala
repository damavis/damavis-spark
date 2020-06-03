package com.damavis.spark.resource.file

import java.time.LocalDate

import com.damavis.spark.resource.Format.Format
import org.apache.spark.sql.SparkSession

private[resource] case class FileReaderParameters(
    format: Format,
    path: String,
    sparkSession: SparkSession,
    datePartitioned: Boolean = false,
    from: Option[LocalDate] = None,
    to: Option[LocalDate] = None)
