package com.bauxite.reporting.sessinizer

import java.util.{Date, UUID}
import java.text.SimpleDateFormat

import scala.collection.mutable.ListBuffer
import scala.math.{max, min}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}
import com.bauxite.reporting.domain.{Event, EventWithSessionInfo}

class EventSessionAggregator extends Aggregator[Event, List[SessionAggregationBufferItem], List[EventWithSessionInfo]] {

  override def zero: List[SessionAggregationBufferItem] = List.empty
  //TODO: review performance after first runs - think about replace plain list to more smart container (better for search in reduce and merge)

  override def reduce(currentBuffer: List[SessionAggregationBufferItem], inputEvent: Event): List[SessionAggregationBufferItem] = {
    val eventTs = inputEvent.getEventUnixTine()
    if (currentBuffer.isEmpty) {
      return List(SessionAggregationBufferItem(List(inputEvent), eventTs, eventTs))
    } else {
      val (eventInsideSessions, otherSessions) = currentBuffer.partition(i => {
        val possibleSessionStartTs = i.sessionStartTs - 300
        val possibleSessionEndTs = i.sessionEndTs + 300

        eventTs >= possibleSessionStartTs && eventTs <= possibleSessionEndTs
      })

      if (eventInsideSessions.isEmpty) {
        val currBufferItem = SessionAggregationBufferItem(List(inputEvent), eventTs, eventTs)
        return currBufferItem :: currentBuffer
      } else {
        val firstSession = eventInsideSessions.head

        val updatedBufferItem = firstSession.copy(
          events = inputEvent :: firstSession.events,
          sessionStartTs = min(firstSession.sessionStartTs, eventTs),
          sessionEndTs = max(firstSession.sessionEndTs, eventTs)
        )

        return updatedBufferItem :: eventInsideSessions.tail ::: otherSessions
      }
    }
  }

  override def merge(lBuffer: List[SessionAggregationBufferItem], rBuffer: List[SessionAggregationBufferItem]): List[SessionAggregationBufferItem] = {
    val allSessions: List[SessionAggregationBufferItem] = lBuffer ++ rBuffer
    val sorted = allSessions.sortBy(r => (r.sessionStartTs, r.sessionEndTs))

    if (sorted == Nil)
      return Nil

    var baseSession = sorted.head
    var otherSessions = sorted.tail

    val result = new ListBuffer[SessionAggregationBufferItem]

    while (otherSessions != Nil) {
      val toMergeSession = otherSessions.head
      otherSessions = otherSessions.tail

      if (baseSession.sessionEndTs + 300 >= toMergeSession.sessionStartTs) {
        baseSession = SessionAggregationBufferItem(
          baseSession.events ++ toMergeSession.events
          ,min(baseSession.sessionStartTs, toMergeSession.sessionStartTs)
          ,max(baseSession.sessionEndTs, toMergeSession.sessionEndTs)
        )
      } else {
        result.append(baseSession)
        baseSession = toMergeSession
      }
    }

    result.append(baseSession)
    result.toList
  }

  private def castFromUnixTimeToISOString(unixTm: Long): String = {
    val dt: Date = new Date(unixTm*1000L);
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    sdf.format(dt)
  }

  override def finish(reduction: List[SessionAggregationBufferItem]): List[EventWithSessionInfo] = {
    reduction.flatMap(b => {
      val sessionId = UUID.randomUUID().toString
      val sessionStartTimeStr = castFromUnixTimeToISOString(b.sessionStartTs)
      val sessionEndTimeStr = castFromUnixTimeToISOString(b.sessionEndTs)

      b.events.map(e => EventWithSessionInfo(
        e.Category, e.Product, e.UserId, e.EventTime, e.EventType
        ,sessionId, sessionStartTimeStr, sessionEndTimeStr
      )
      )
    }
    )
  }

  override def bufferEncoder: Encoder[List[SessionAggregationBufferItem]] = Encoders.product[List[SessionAggregationBufferItem]]

  override def outputEncoder: Encoder[List[EventWithSessionInfo]] = Encoders.product[List[EventWithSessionInfo]]

}
