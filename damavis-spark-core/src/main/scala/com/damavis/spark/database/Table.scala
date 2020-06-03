package com.damavis.spark.database

import com.damavis.spark.resource.Format.Format

case class Column(name: String,
                  dataType: String,
                  partitioned: Boolean,
                  nullable: Boolean)

sealed trait Table {
  def database: String
  def name: String
  def path: String
  def format: Format
  def managed: Boolean
  def columns: Seq[Column]

  def isPartitioned: Boolean = columns.indexWhere(_.partitioned) >= 0
}

case class RealTable(database: String,
                     name: String,
                     path: String,
                     format: Format,
                     managed: Boolean,
                     columns: Seq[Column])
    extends Table

case class DummyTable(database: String, name: String) extends Table {
  override def path: String = ???
  override def format: Format = ???
  override def managed: Boolean = ???
  override def columns: Seq[Column] = ???
}
