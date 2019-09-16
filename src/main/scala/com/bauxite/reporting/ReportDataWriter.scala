package com.bauxite.reporting

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode

class ReportDataWriter {
  def write(data: Dataset[Row], path: String) = {
    data
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("sep", "\t")
      .option("header", "false")
      .csv(path)
  }
}
