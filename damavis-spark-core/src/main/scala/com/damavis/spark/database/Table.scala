package com.damavis.spark.database

import com.damavis.spark.resource.datasource.enums.Format.Format

case class Table(database: String,
                 name: String,
                 //TODO: can partition columns of external table be determined from catalog?
                 path: String,
                 format: Format,
                 managed: Boolean)
