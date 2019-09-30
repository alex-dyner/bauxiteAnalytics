package com.bauxite.reporting

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Simple reader of source data. Read event data from the storage platform
 * @param sparkSession entry point to Spark runtime.
 */
class InputDataProvider(sparkSession: SparkSession) {

  /**
   * Load source data to the Spark.
   * @param path Source data input path
   * @param delimiter Delimiter symbol
   * @return
   */
  def getEvents(path: String, delimiter: String = "\t"): DataFrame = {
    val schema = EventDataStructureInfo.getEventSchema()

    sparkSession.read.format("csv")
      .option("delimiter", delimiter)
      .option("header", "false")
      .option("inferSchema", "false")
      .schema(schema)
      .load(path)
  }

}
