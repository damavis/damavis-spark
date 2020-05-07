package com.damavis.spark.database

import org.apache.spark.sql.catalog.Catalog

class Schema(catalog: Catalog) {

  def tableExists(table: String): Boolean =
    catalog.tableExists(table)

}
