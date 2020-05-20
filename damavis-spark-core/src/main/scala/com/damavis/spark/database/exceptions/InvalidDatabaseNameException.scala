package com.damavis.spark.database.exceptions

class InvalidDatabaseNameException(name: String)
    extends Exception(s""""$name" is not a valid database name""") {}
