package com.bauxite.reporting

import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
 * Event data structure keeper.
 */
object EventDataStructureInfo {

  /**
   * Provides event data structure
   * @return
   */
  def getEventSchema(): StructType = {
    StructType(
      Array(
        StructField("category", StringType, false),
        StructField("product", StringType, false),
        StructField("user_id", StringType, false),
        StructField("event_time", StringType, false),
        StructField("event_type", StringType, false),
      )
    )
  }

}