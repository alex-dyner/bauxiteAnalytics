package com.bauxite.reporting

import org.apache.spark.sql.types.{StringType, StructField, StructType}

object EventDataStructureInfo {

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
