package com.bauxite.reporting.sessinizer

import com.bauxite.reporting.EventDataStructureInfo
import com.bauxite.reporting.domain.Event
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers, PrivateMethodTester}

class EventSessionizerTest extends FunSuite with Matchers with BeforeAndAfter with PrivateMethodTester {

  var sparkSession: SparkSession = _

  before {
    sparkSession = SparkSession
      .builder()
      .master("local[2]")
      .appName("test")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }


  test("testGetSessionizedEvents") {
    val localSparkSession = sparkSession
    import localSparkSession.implicits._

    val testDf = getTestEventDF()
    val testDS: Dataset[Event] = testDf.map(r =>
      Event(r.getString(0), r.getString(1), r.getString(2), r.getString(3), r.getString(4))
    )

    val result = EventSessionizer.getSessionizedEvents(testDS)

    val data = result.collect()

    testDf.count() shouldBe data.size
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


  after {
    sparkSession.stop()
  }

}
