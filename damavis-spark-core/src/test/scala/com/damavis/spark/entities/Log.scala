package com.damavis.spark.entities

import java.sql.Timestamp

case class Log(ip: String, ts: Timestamp, level: String, message: String)
