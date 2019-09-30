package com.bauxite.reporting

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println(
        """
          |Usage: {command-to-start-app} {mode} {path-to-source-data} {path-to-result-dir}
          |Mode = {SessionizedEvents||MedianSessionDurationByCategory|DurationHistogramByCategory|Top10ProductByCategory}
          |""".stripMargin
      )
      System.exit(1)
    }

    val mode = args(0)
    val inputPath = args(1)
    val outputPath = args(2)

    val sparkSession = SparkSession
      .builder()
      .appName("bauxiteSalesReport")
      .getOrCreate()

    val reportAdapter = new ReportAdapter(sparkSession, inputPath)

    val reportData = if (mode == "SessionizedEventsViaSparkAgg") {
      reportAdapter.getSessionizedEventsByAggr()
    } else {
      reportAdapter.getReportData(mode)
    }

    (new ReportDataWriter()).write(reportData, outputPath)

    sparkSession.stop()
  }
}