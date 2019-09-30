package com.bauxite.reporting

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class InputDataProvider(sparkSession: SparkSession) {

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
