package com.damavis.spark.resource
import java.time.LocalDate

import org.apache.spark.sql.{DataFrame, SparkSession}

object ParquetPartitionedReader {
  def apply(path: String, spark: SparkSession): ParquetPartitionedReader = {
    val parquetReader = spark.read
      .option("basePath", path)
      .format("parquet")

    val params = ParquetReaderParameters(path, parquetReader, spark)
    new ParquetPartitionedReader(params)
  }
}

class ParquetPartitionedReader(params: ParquetReaderParameters)
    extends ResourceReader {
  override def read(): DataFrame = {
    if (!(params.from.isDefined && params.to.isDefined)) {
      val errMsg =
        s"""Invalid parameters defined for reading spark object with path: ${params.path}.
           |Missing either "from" or "to" date (or both)
           |""".stripMargin
      throw new RuntimeException(errMsg)
    }

    val from = params.from.get
    val to = params.to.get

    if (from.isAfter(to)) {
      val errMsg =
        s"""Invalid parameters defined for reading spark object with path: ${params.path}.
           |"from" date is after "to" date.
           |Dates are: from=$from to=$to
           |""".stripMargin
      throw new RuntimeException(errMsg)
    }

    implicit val spark: SparkSession = params.sparkSession
    params.sparkReader.load(DatePaths.generate(params.path, from, to): _*)
  }

  def from(date: LocalDate): ParquetPartitionedReader =
    new ParquetPartitionedReader(params.copy(from = Some(date)))

  def to(date: LocalDate): ParquetPartitionedReader =
    new ParquetPartitionedReader(params.copy(to = Some(date)))
}
