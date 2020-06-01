package com.damavis.spark.database

import com.damavis.spark.resource.datasource.enums.Format.Format

case class PartitionColumn(name: String, dataType: String, nullable: Boolean)

sealed trait Table {
  def database: String
  def name: String
  def path: String
  def format: Format
  def managed: Boolean
  def partitions: Seq[PartitionColumn]

  def isPartitioned: Boolean = partitions.nonEmpty
}

case class RealTable(database: String,
                     name: String,
                     path: String,
                     format: Format,
                     managed: Boolean,
                     partitions: Seq[PartitionColumn])
    extends Table

case class DummyTable(database: String, name: String) extends Table {
  override def path: String = ???
  override def format: Format = ???
  override def managed: Boolean = ???
  override def partitions: Seq[PartitionColumn] = ???
}
