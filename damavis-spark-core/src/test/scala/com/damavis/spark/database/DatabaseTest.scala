package com.damavis.spark.database

import com.damavis.spark.utils.SparkTest

class DatabaseTest extends SparkTest {

  //val db = DbManager.useDatabase("test", forceCreation = true)

  "A database" should {
    "create a table with specified parameters" in {
      val db = DbManager.useDatabase("test", forceCreation = true)
      /*db.catalog.createTable("atable", "/at_root/")

      db.catalog.listTables().show() */
      //db.prepareTable("table1", TableOptions())
    }

    "report whether a table exists or not" in {}

    "recover specified table along with its metadata" in {}

    "recover table successfully ignoring database from table name" in {}

    "fail to recover a table that does not exists" in {}

    "fail to recover a table that belongs to another database" in {}
  }
}
