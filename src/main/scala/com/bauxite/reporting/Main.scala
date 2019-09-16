package com.bauxite.reporting

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println(
        """
          |Usage: {command-to-start-app} {mode} {path-to-source-data} {path-to-result-dir}
          |Mode = {SessionizedEvents|MedianSessionDurationByCategory|DurationHistogramByCategory|Top10ProductByCategory}
          |""".stripMargin
      )
      System.exit(1)
    }

    val mode = args(0)
    val inputPath = args(1)

    val sparkSession = SparkSession
      .builder()
      .appName("bauxiteSalesReport")
      .getOrCreate()

    val inputData = new InputDataProvider(sparkSession).getEvents(inputPath)

    val reportDataProcessor = new ReportDataProcessor(sparkSession)
    val reportData = reportDataProcessor.getReportData(inputData, mode);

    if (reportData != null) {
      val outputPath = args(2)
      (new ReportDataWriter()).write(reportData, outputPath)
    } else {
      println("Unexpected mode:" + mode)
    }

    sparkSession.stop()
  }
}
