package com.damavis.spark.resource.parquet

import java.time.LocalDate

import com.damavis.spark.resource.{ReaderBuilder, ResourceReader}

class PartitionedParquetReaderBuilder(params: ParquetReaderParameters)
    extends ReaderBuilder {
  override def reader(): ResourceReader = {
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

    new ParquetPartitionedReader(params)
  }

  def from(date: LocalDate): PartitionedParquetReaderBuilder =
    new PartitionedParquetReaderBuilder(params.copy(from = Some(date)))

  def to(date: LocalDate): PartitionedParquetReaderBuilder =
    new PartitionedParquetReaderBuilder(params.copy(to = Some(date)))
}
