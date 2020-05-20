package com.damavis.spark.database.exceptions

class DatabaseNotFoundException(name: String)
    extends Exception(s"""Database "$name" not found in catalog""") {}
