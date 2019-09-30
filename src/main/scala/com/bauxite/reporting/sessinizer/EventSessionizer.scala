package com.bauxite.reporting.sessinizer

import com.bauxite.reporting.domain.{Event, EventWithSessionInfo}
import org.apache.spark.sql.{DataFrame, Dataset, RelationalGroupedDataset, Row}

object EventSessionizer {
  def getSessionizedEvents(eventData: Dataset[Event]): Dataset[EventWithSessionInfo]= {
    import eventData.sparkSession.implicits._

    val sessionAgg = (new EventSessionAggregator()).toColumn

    eventData
      .groupByKey(x => (x.UserId, x.Category))
      .agg(sessionAgg)
      .flatMap(_._2)
  }
}
