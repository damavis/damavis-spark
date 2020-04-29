package com.damavis.spark.resource

trait WriterBuilder {
  def writer(): ResourceWriter
}
