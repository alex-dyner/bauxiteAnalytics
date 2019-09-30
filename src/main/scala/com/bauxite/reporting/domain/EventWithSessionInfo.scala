package com.bauxite.reporting.domain

final case class EventWithSessionInfo (
                           Category: String
                          ,Product: String
                          ,UserId: String
                          ,EventTime: String
                          ,EventType: String

                          ,SessionId: String
                          ,SessionStartTs: String
                          ,SessionEndTs: String
)

