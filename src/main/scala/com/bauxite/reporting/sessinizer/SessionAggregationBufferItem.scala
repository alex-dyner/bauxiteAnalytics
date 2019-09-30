package com.bauxite.reporting.sessinizer

import com.bauxite.reporting.domain.Event

/**
 * Buffer for session aggregator
 * @param events Current list of session event
 * @param sessionStartTs Session end time in Unix time.
 * @param sessionEndTs Session end time in Unix time.
 */
final case class SessionAggregationBufferItem(
  //TODO: review performance after first runs - think about replace plain list to more smart container (better for search in reduce and merge)
  events: List[Event],
  sessionStartTs: Long,
  sessionEndTs: Long
)
