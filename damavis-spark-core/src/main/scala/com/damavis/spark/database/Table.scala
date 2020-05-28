package com.damavis.spark.database

import com.damavis.spark.resource.datasource.enums.Format.Format

sealed trait Table {
  //TODO: can partition columns of external table be determined from catalog?

  def database: String
  def name: String
  def path: String
  def format: Format
  def managed: Boolean
}

case class ConcreteTable(database: String,
                         name: String,
                         path: String,
                         format: Format,
                         managed: Boolean)
    extends Table

case class DummyTable(database: String, name: String) extends Table {
  override def path: String = ???
  override def format: Format = ???
  override def managed: Boolean = true
}
