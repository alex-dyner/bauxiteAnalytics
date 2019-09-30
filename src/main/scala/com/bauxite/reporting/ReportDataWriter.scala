package com.bauxite.reporting

import com.bauxite.reporting.domain.EventWithSessionInfo
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode

/** Simple writer of report data. Write abstract data to the storage platform */
class ReportDataWriter {

  /** Writes input data into a file system in TSV format.
   * @param data input dataset
   * @param path path on a file system
   */
  def write[T](data: Dataset[T], path: String) = {
    data
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("sep", "\t")
      .option("header", "false")
      .csv(path)
  }
}
