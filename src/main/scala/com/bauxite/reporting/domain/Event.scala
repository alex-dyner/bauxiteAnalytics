package com.bauxite.reporting.domain

import java.text.SimpleDateFormat

final case class Event(
                        Category: String
                        ,Product: String
                        ,UserId: String
                        ,EventTime: String
                        ,EventType: String
                      )
{
  private val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def getEventUnixTine(): Long = {
    val tm = format.parse(EventTime)
    tm.getTime / 1000
  }
}
