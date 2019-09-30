package com.bauxite.reporting

import com.bauxite.reporting.domain.{Event, EventWithSessionInfo}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

class ReportAdapter(sparkSession: SparkSession, inputEventPath: String) {
  private val processor: ReportDataProcessor = new ReportDataProcessor(sparkSession)
  private val events: DataFrame = new InputDataProvider(sparkSession).getEvents(inputEventPath)

  private val defaultSessionDimensions = List("user_id", "category")

  def getTop10ProductByCategory(): Dataset[Row] = {
    val sessions = processor.getSessions(events, List("user_id", "category", "product"))
    processor.getTop10ProductByCategory(sessions)
  }

  def getMedianSessionDurationByCategory(): Dataset[Row] = {
    val sessions = processor.getSessions(events, defaultSessionDimensions)
    processor.getMedianSessionDurationByCategory(sessions)
  }

  def getDurationHistogramByCategory(): Dataset[Row] = {
    val sessions = processor.getSessions(events, defaultSessionDimensions)
    processor.getDurationHistogramByCategory(sessions)
  }

  def getReportData(mode: String): Dataset[Row] = {
    mode match {
      case "SessionizedEvents" => processor.getSessionizedEvents(events, List("user_id", "category"))
      case "Top10ProductByCategory" => this.getTop10ProductByCategory()
      case "MedianSessionDurationByCategory" => this.getMedianSessionDurationByCategory()
      case "DurationHistogramByCategory" => this.getDurationHistogramByCategory()
      case _ => throw new IllegalArgumentException("Wrong mode");
    }
  }

  def getSessionizedEventsByAggr(): Dataset[EventWithSessionInfo] = {
    import events.sparkSession.implicits._
    val ds: Dataset[Event] = events.map(r =>
      Event(r.getString(0), r.getString(1), r.getString(2), r.getString(3), r.getString(4))
    )
    sessinizer.EventSessionizer.getSessionizedEvents(ds)
  }
}
