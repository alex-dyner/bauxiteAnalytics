package com.bauxite.reporting

import org.scalatest._
import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

class ReportDataProcessorTest extends FunSuite with Matchers with BeforeAndAfter {

  var sparkSession: SparkSession = _

  before {
    sparkSession = SparkSession
      .builder()
      .master("local[2]")
      .appName("test")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  test ("getSessionizedEvents") {
    val df: DataFrame = getTestEventDF()
    val sessionizer = new ReportDataProcessor(sparkSession)
    val keyCols: Seq[String] = List("user_id", "category")
    val output: Dataset[Row] = sessionizer.getSessionizedEvents(df, keyCols)

    val expectedOutputSchema = StructType(
      Array(
        StructField("category", StringType, false),
        StructField("product", StringType, false),
        StructField("user_id", StringType, false),
        StructField("event_time", StringType, false),
        StructField("event_type", StringType, false),
        StructField("session_id", StringType, true),
        StructField("session_start_ts", StringType, true),
        StructField("session_end_ts", StringType, true)
      )
    )

    output.schema shouldBe expectedOutputSchema

    val outputData: Array[Row] = output.collect()

    val eventCount = getTestEventData().size
    outputData.length shouldBe eventCount

    val keyColsExtr: Seq[String] = List("user_id", "category", "product")

    val eventSessionizedWithProductDF = sessionizer.getSessionizedEvents(df, keyColsExtr)
    val eventSessionizedWithProduct = eventSessionizedWithProductDF.collect()

    eventSessionizedWithProductDF.schema shouldBe expectedOutputSchema

    eventSessionizedWithProduct.length shouldBe eventCount
  }


  test ("getTop10ProductByCategory") {
    val rawData: Seq[Row] = Seq (
      Row("00", "U1", "C1", "P01", 1L, 100L),
      Row("01", "U2", "C1", "P02", 1L, 150L),
      Row("02", "U2", "C1", "P03", 1L, 200L),
      Row("03", "U2", "C1", "P04", 1L, 300L),
      Row("04", "U3", "C1", "P05", 1L, 400L),
      Row("09", "U2", "C1", "P06", 1L, 500L),
      Row("10", "U1", "C1", "P07", 1L, 600L),
      Row("11", "U2", "C1", "P08", 1L, 700L),
      Row("12", "U2", "C1", "P09", 1L, 800L),
      Row("13", "U2", "C1", "P10", 1L, 900L),

      Row("14", "U2", "C1", "P11", 1L, 500L),
      Row("15", "U3", "C1", "P11", 1L, 500L),
      Row("16", "U2", "C1", "P11", 1L, 500L),

      Row("16", "U2", "C2", "P21", 1L, 400L),
      Row("16", "U2", "C2", "P22", 1L, 700L),
      Row("16", "U2", "C2", "P22", 1L, 800L),
    )
    val schema = StructType(
        Array(
          StructField("session_id", StringType, false),
          StructField("user_id", StringType, false),
          StructField("category", StringType, false),
          StructField("product", StringType, false),
          StructField("session_start_ts", LongType, false),
          StructField("session_end_ts", LongType, false),
        )
    )
    val inputDF = buildDF(rawData, schema)
    val sessionizer = new ReportDataProcessor(sparkSession)
    val outputData = sessionizer.getTop10ProductByCategory(inputDF).collect()
    val mappedOutputData: Map[(String, Int), Row] = outputData.map(r => ((r.getString(0), r.getInt(3)), r)).toMap

    mappedOutputData.size shouldBe 12
    mappedOutputData(("C1", 1)).getString(1) shouldBe "P11"
    mappedOutputData(("C1", 10)).getString(1) shouldBe "P02"

    mappedOutputData(("C2", 1)).getString(1) shouldBe "P22"
  }

  test("getDurationHistogramByCategory") {
    val rawData: Seq[Row] = Seq (
      Row("0", "U1", "C1", 100L, 150L),
      Row("1", "U2", "C1", 100L, 300L),
      Row("2", "U2", "C1", 100L, 500L),

      Row("3", "U2", "C2", 100L, 300L),
      Row("4", "U3", "C2", 100L, 300L),

      Row("9", "U2", "C3", 100L, 100L),
    )

    val inputDF = buildDF(rawData, getSessionSchema)
    val sessionizer = new ReportDataProcessor(sparkSession)
    val outputData = sessionizer.getDurationHistogramByCategory(inputDF).collect()
    val mappedOutputData: Map[String, Row] = outputData.map(r => (r.getString(0), r)).toMap

    mappedOutputData.size shouldBe 3

    mappedOutputData("C1") shouldBe Row("C1", 1, 1, 1)
    mappedOutputData("C2") shouldBe Row("C2", 0, 2, 0)
    mappedOutputData("C3") shouldBe Row("C3", 1, 0, 0)
  }

  test( "getMedianSessionDurationByCategory" ) {
    val rawData: Seq[Row] = Seq (
        Row("0", "U1", "C1", 100L, 200L),
        Row("1", "U2", "C1", 100L, 300L),
        Row("2", "U2", "C1", 100L, 400L),
        Row("3", "U2", "C2", 100L, 300L),
        Row("9", "U2", "C3", 100L, 100L),
    )

    val inputDF = buildDF(rawData, getSessionSchema)
    val sessionizer = new ReportDataProcessor(sparkSession)
    val outputData = sessionizer.getMedianSessionDurationByCategory(inputDF).collect()

    val mappedOutputData: Map[String, Row] = outputData.map(r => (r.getString(0), r)).toMap
    mappedOutputData.size shouldBe 3

    mappedOutputData("C1")(1) shouldBe 200L
    mappedOutputData("C2")(1) shouldBe 200L
    mappedOutputData("C3")(1) shouldBe 0L
  }

  test("testGetHardBorderSessions") {
    val df = getTestEventDF

    val sessionizer = new ReportDataProcessor(sparkSession)
    val keyCols: Seq[String] = List("user_id", "category")
    val hardBorderSessions = sessionizer.getHardBorderSessions(df, keyCols).collect()

    val mappedSessions: Map[String, Row] = hardBorderSessions.map(r => (r.getString(0), r)).toMap

    mappedSessions.size shouldBe 10

    val user100Key = "#user 100#books#" + castToUnixTime("2018-03-01 12:00:02")
    mappedSessions.contains(user100Key) shouldBe true

    val user400Key = "#user 400#notebooks#" + castToUnixTime("2018-03-01 12:20:10")
    mappedSessions.contains(user400Key) shouldBe true
  }

  private def castToUnixTime(input: String) = {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val user100StartTm = format.parse(input)
    val user100StartUnixTime = user100StartTm.getTime / 1000
    user100StartUnixTime.toString
  }

  private def buildDF(data: Seq[Row], schema: StructType): DataFrame ={
    val dataRDD: RDD[Row] = sparkSession.sparkContext.parallelize(data)
    sparkSession.createDataFrame(dataRDD, schema)
  }

  private def getTestEventDF(): DataFrame = {
    val data = getTestEventData
    val schema: StructType = EventDataStructureInfo.getEventSchema()
    buildDF(data, schema)
  }

  private def getTestEventData(): Seq[Row] = {
    Seq(
      Row("books", "Scala for Dummies", "user 100", "2018-03-01 12:00:02", "view description"),
      Row("books", "Scala for Dummies", "user 100", "2018-03-01 12:01:40", "like"),
      Row("books", "Scala for Dummies", "user 100", "2018-03-01 12:01:50", "check status"),
      Row("books", "Java for Dummies", "user 100", "2018-03-01 12:02:14", "view description"),
      Row("books", "Java for Dummies", "user 100", "2018-03-01 12:02:45", "dislike"),
      Row("books", "Java for Dummies", "user 100", "2018-03-01 12:02:50", "close description"),
      Row("books", "Scala for Dummies", "user 100", "2018-03-01 12:04:30", "view description"),
      Row("books", "Scala for Dummies", "user 100", "2018-03-01 12:06:15", "add to bucket"),
      Row("books", "Sherlock Holmes, full collection", "user 200", "2018-03-01 12:11:25", "view description"),
      Row("books", "Sherlock Holmes, full collection", "user 200", "2018-03-01 12:12:15", "check status"),
      Row("books", "Sherlock Holmes, full collection", "user 200", "2018-03-01 12:13:15", "sign for updates"),
      Row("books", "Romeo and Juliet", "user 200", "2018-03-01 12:15:10", "view description"),
      Row("books", "Romeo and Juliet", "user 200", "2018-03-01 12:15:10", "check status"),
      Row("books", "Romeo and Juliet", "user 200", "2018-03-01 12:15:10", "add to bucket"),
      Row("mobile phones", "iPhone X", "user 300", "2018-03-01 12:05:03", "view description"),
      Row("mobile phones", "iPhone X", "user 300", "2018-03-01 12:05:20", "add to bucket"),
      Row("mobile phones", "iPhone 8 Plus", "user 100", "2018-03-01 12:06:49", "view description"),
      Row("mobile phones", "iPhone 8 Plus", "user 100", "2018-03-01 12:07:14", "add to bucket"),
      Row("mobile phones", "iPhone 8", "user 100", "2018-03-01 12:08:01", "view description"),
      Row("mobile phones", "iPhone 8", "user 100", "2018-03-01 12:13:10", "dislike"),
      Row("notebooks", "MacBook Pro 15", "user 100", "2018-03-01 12:15:08", "view description"),
      Row("notebooks", "MacBook Pro 15", "user 100", "2018-03-01 12:16:10", "check status"),
      Row("notebooks", "MacBook Pro 13", "user 200", "2018-03-01 12:15:33", "view description"),
      Row("notebooks", "MacBook Pro 13", "user 200", "2018-03-01 12:17:25", "like"),
      Row("notebooks", "MacBook Pro 13", "user 200", "2018-03-01 12:17:56", "add to bucket"),
      Row("notebooks", "MacBook Air", "user 300", "2018-03-01 12:14:58", "view description"),
      Row("notebooks", "MacBook Air", "user 300", "2018-03-01 12:20:10", "dislike"),

      Row("notebooks", "MacBook Air", "user 400", "2018-03-01 12:20:10", "dislike"),
    )
  }

  private def getSessionSchema() = {
    StructType(
      Array(
        StructField("session_id", StringType, false),
        StructField("user_id", StringType, false),
        StructField("category", StringType, false),
        StructField("session_start_ts", LongType, false),
        StructField("session_end_ts", LongType, false),
      )
    )
  }

  after {
    sparkSession.stop()
  }
}
