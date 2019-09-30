package com.bauxite.reporting.sessinizer

import com.bauxite.reporting.domain.{Event, EventWithSessionInfo}
import org.apache.spark.sql.{DataFrame, Dataset, RelationalGroupedDataset, Row}

/**
 * Simple tool for enrich wvent data.
 */
object EventSessionizer {

  /**
   * Get enriched events with session information. Sessions dimension = UserId and Category.
   * @param eventData Source event data.
   * @return Enriches source events with session data.
   */
  def getSessionizedEvents(eventData: Dataset[Event]): Dataset[EventWithSessionInfo]= {
    import eventData.sparkSession.implicits._

    val sessionAgg = (new EventSessionAggregator()).toColumn

    eventData
      .groupByKey(x => (x.UserId, x.Category))
      .agg(sessionAgg)
      .flatMap(_._2)
  }
}
