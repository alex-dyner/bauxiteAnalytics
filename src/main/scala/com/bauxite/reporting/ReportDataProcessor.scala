package com.bauxite.reporting

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

class ReportDataProcessor(sparkSession: SparkSession) {

  def getReportData(eventData: DataFrame, mode: String): Dataset[Row] = {
    mode match {
      case "SessionizedEvents" => this.getSessionizedEvents(eventData, List("user_id", "category"))
        case "Top10ProductByCategory" => {
        val intermediateResult = this.getSessions(eventData, List("user_id", "category", "product"))
        this.getTop10ProductByCategory(intermediateResult)
      }
      case "MedianSessionDurationByCategory" | "DurationHistogramByCategory" => {
        val sessions = this.getSessions(eventData, List("user_id", "category"))
        if (mode == "MedianSessionDurationByCategory") {
          this.getMedianSessionDurationByCategory(sessions)
        } else {
          this.getDurationHistogramByCategory(sessions)
        }
      }
      case _ => null
    }
  }

  def getTop10ProductByCategory(productSession: DataFrame): Dataset[Row] = {
    productSession.createOrReplaceTempView("sessions")

    val sqlText = """
        |SELECT *
        |FROM (
        |    SELECT
        |         category
        |        ,product
        |        ,total_session_duration_sec
        |        ,rank() OVER (PARTITION BY category ORDER BY total_session_duration_sec DESC) as product_rank
        |    FROM (
        |        SELECT
        |             category
        |            ,product
        |            ,sum(session_end_ts - session_start_ts) AS total_session_duration_sec
        |        FROM sessions
        |        GROUP BY category, product
        |    ) s
        |) src
        |WHERE product_rank <= 10
        |""".stripMargin

    val result = sparkSession.sql(sqlText)
    sparkSession.catalog.dropTempView("sessions")
    result
  }

  def getDurationHistogramByCategory(sessions: DataFrame): Dataset[Row] = {
    sessions.createOrReplaceTempView("sessions")

    val sqlText ="""
        |SELECT
        |     category
        |    ,count(DISTINCT CASE WHEN session_kind_code = 0 THEN user_id ELSE NULL END) AS session_less_1min_cnt
        |    ,count(DISTINCT CASE WHEN session_kind_code = 1 THEN user_id ELSE NULL END) AS session_less_5min_cnt
        |    ,count(DISTINCT CASE WHEN session_kind_code = 2 THEN user_id ELSE NULL END) AS session_more_5min_cnt
        |FROM (
        |    SELECT
        |         user_id
        |        ,category
        |        ,CASE
        |            WHEN session_duration_sec < 60 THEN 0
        |            WHEN session_duration_sec < 300 THEN 1
        |            ELSE 2
        |        END as session_kind_code
        |    FROM (
        |        SELECT
        |            sessn.*
        |            ,session_end_ts - session_start_ts AS session_duration_sec
        |        FROM sessions sessn
        |    ) s
        |) src
        |GROUP BY category
        |""".stripMargin

      sparkSession.sql(sqlText)

    val result = sparkSession.sql(sqlText)
    sparkSession.catalog.dropTempView("sessions")
    result
  }

  def getMedianSessionDurationByCategory(sessions: DataFrame): Dataset[Row] ={
    sessions.createOrReplaceTempView("sessions")

    val sqlText = """
      |SELECT category, percentile_approx(session_end_ts - session_start_ts, 0.5) AS duration_median
      |FROM sessions
      |GROUP BY category
      |""".stripMargin
    sparkSession.sql(sqlText)

    val result = sparkSession.sql(sqlText)
    sparkSession.catalog.dropTempView("sessions")
    result
  }

  def getHardBorderSessions(eventData: DataFrame, sessionKeys: Seq[String]): Dataset[Row] ={
    eventData.createOrReplaceTempView("events")

    val keyColumnList = sessionKeys.mkString(",")
    val imageOfKeyColumnList = sessionKeys.map(c => "'#' || CAST(" + c + " AS VARCHAR(2048))").mkString("||")

    val sqlText = """
        |SELECT
        |     session_id
        |    ,{key_column_list}
        |    ,session_start_ts
        |    ,lead(session_start_ts) OVER (PARTITION BY {key_column_list} ORDER BY session_start_ts) - 1 AS session_end_ts
        |FROM (
        |    SELECT
        |         {key_column_list}
        |        ,{image_key_column_list} || "#" || CAST(event_ts as VARCHAR(100)) AS session_id
        |        ,event_ts AS session_start_ts
        |    FROM (
        |        SELECT
        |            e.*
        |            ,lag(event_ts) OVER (PARTITION BY {key_column_list} ORDER BY event_ts) as prev_event_ts
        |        FROM (
        |            SELECT
        |                 inpt.*
        |                ,unix_timestamp(event_time) as event_ts
        |            FROM events inpt
        |        ) e
        |    ) sessn
        |    WHERE
        |        prev_event_ts IS NULL
        |        OR (event_ts - prev_event_ts) > 300
        |) src
        |""".stripMargin
      .replaceAll("\\{key_column_list\\}", keyColumnList)
      .replaceAll("\\{image_key_column_list\\}", imageOfKeyColumnList)

    val result = sparkSession.sql(sqlText)
    sparkSession.catalog.dropTempView("events")
    result
  }

  def getSessions(eventData: DataFrame, sessionKeys: Seq[String]): Dataset[Row] = {
    val draftSessions = getHardBorderSessions(eventData, sessionKeys)
    draftSessions.createOrReplaceTempView("sessions")
    eventData.createOrReplaceTempView("events")

    val keyJoinCondition = sessionKeys.map(columnName => "s." + columnName + " = e." + columnName).mkString(" AND ")
    val keyCols = sessionKeys.map(colName => "s." + colName).mkString(",")

    val sqlText = """
        |SELECT
        |     {key_cols}
        |    ,s.session_id
        |    ,min(s.session_start_ts) AS session_start_ts
        |    ,max(e.event_ts) AS session_end_ts
        |FROM
        |    sessions s
        |    INNER JOIN (
        |        SELECT
        |            inpt.*
        |            ,unix_timestamp(event_time) as event_ts
        |        FROM events inpt
        |    ) e
        |    ON 1 = 1
        |        AND ({key_join_condition})
        |        AND e.event_ts >= s.session_start_ts
        |        AND e.event_ts <= coalesce(s.session_end_ts, 999999999999999)
        |GROUP BY {key_cols}, session_id
        |""".stripMargin
      .replaceAll("\\{key_join_condition\\}", keyJoinCondition)
      .replaceAll("\\{key_cols\\}", keyCols)

    sparkSession.sql(sqlText)
  }

  def getSessionizedEvents(eventData: DataFrame, sessionKeys: Seq[String]): Dataset[Row] = {
    val draftSessions = getHardBorderSessions(eventData, sessionKeys)

    draftSessions.createOrReplaceTempView("draft_sessions")
    eventData.createOrReplaceTempView("events")

    val keyJoinCondition = sessionKeys.map(columnName => "s." + columnName + " = e." + columnName).mkString(" AND ")

    val sqlText = """
        |SELECT
        |     e.category
        |    ,e.product
        |    ,e.user_id
        |    ,e.event_time
        |    ,e.event_type
        |    ,s.session_id
        |    ,from_unixtime(s.session_start_ts, "yyyy-MM-dd HH:mm:ss") AS session_start_ts
        |    ,from_unixtime(
        |         MAX(event_ts) OVER (PARTITION BY session_id)
        |        ,"yyyy-MM-dd HH:mm:ss"
        |    ) AS session_end_ts
        |FROM
        |    draft_sessions s
        |    INNER JOIN (
        |        SELECT
        |            inpt.*
        |            ,unix_timestamp(event_time) as event_ts
        |        FROM events inpt
        |    ) e
        |    ON 1 = 1
        |        AND ({key_join_condition})
        |        AND e.event_ts >= s.session_start_ts
        |        AND e.event_ts <= coalesce(s.session_end_ts, 999999999999999)
        |""".stripMargin.replaceAll("\\{key_join_condition\\}", keyJoinCondition)

    val result = sparkSession.sql(sqlText)
    sparkSession.catalog.dropTempView("draft_sessions")
    sparkSession.catalog.dropTempView("events")

    result
  }
}
