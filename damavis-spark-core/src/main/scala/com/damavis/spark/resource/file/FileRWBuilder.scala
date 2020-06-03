package com.damavis.spark.resource.file

import java.time.{LocalDate, LocalDateTime, LocalTime}

import com.damavis.spark.resource.Format.Format
import com.damavis.spark.resource.{BasicResourceRW, RWBuilder, ResourceRW}
import org.apache.spark.sql.SparkSession

object FileRWBuilder {
  def apply(path: String, format: Format)(
      implicit spark: SparkSession): FileRWBuilder = {
    val readParams = FileReaderParameters(format, path)
    val writeParams = FileWriterParameters(format, path)

    new FileRWBuilder(readParams, writeParams)
  }
}

class FileRWBuilder(
    readParams: FileReaderParameters,
    writeParams: FileWriterParameters)(implicit spark: SparkSession)
    extends RWBuilder {
  override def build(): ResourceRW = {
    val reader = FileReaderBuilder(readParams).reader()
    val writer = FileWriterBuilder(writeParams).writer()

    new BasicResourceRW(reader, writer)
  }

  def betweenDates(from: LocalDate, to: LocalDate): FileRWBuilder = {
    val time = LocalTime.of(0, 0, 0)
    betweenDates(LocalDateTime.of(from, time), LocalDateTime.of(to, time))
  }

  def betweenDates(from: LocalDateTime, to: LocalDateTime): FileRWBuilder = {
    val newReadParams =
      readParams.copy(from = Some(from), to = Some(to))

    //TODO: MAKE THIS PARAMETERIZABLE!!!!!
    val newWriteParams =
      writeParams.copy(columnNames = "year" :: "month" :: "day" :: Nil)

    new FileRWBuilder(newReadParams, newWriteParams)
  }

  def writeMode(mode: String): FileRWBuilder =
    new FileRWBuilder(readParams, writeParams.copy(mode = mode))
}
