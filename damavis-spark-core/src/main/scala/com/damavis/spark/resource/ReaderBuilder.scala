package com.damavis.spark.resource

trait ReaderBuilder {
  def reader(): ResourceReader
}
