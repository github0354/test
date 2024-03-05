package test.demo

import org.scalatest._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

class UnitTest extends AnyFunSuite with BeforeAndAfter {
  var spark: SparkSession = _

  before {
    spark = SparkSession.builder()
      .appName("YearlyCountUnitTest")
      .master("local[*]")
      .getOrCreate()
  }

  after {
    if (spark != null) {
      spark.stop()
    }
  }

  test("Test getYearByPeer function") {
    import spark.implicits._

    // Sample data
    val data = Seq(
      ("ABC17969(AB)", "1", "ABC17969", 2022),
      ("ABC17969(AB)", "2", "CDC52533", 2022),
      ("ABC17969(AB)", "3", "DEC59161", 2023),
      ("ABC17969(AB)", "4", "F43874", 2022),
      ("ABC17969(AB)", "5", "MY06154", 2021),
      ("ABC17969(AB)", "6", "MY4387", 2022),
      ("AE686(AE)", "7", "AE686", 2023),
      ("AE686(AE)", "8", "BH2740", 2021),
      ("AE686(AE)", "9", "EG999", 2021),
      ("AE686(AE)", "10", "AE0908", 2021),
      ("AE686(AE)", "11", "QA402", 2022),
      ("AE686(AE)", "12", "OM691", 2022)
    )
    val df = data.toDF("peer_id", "id_1", "id_2", "year")
    val result = PeerYearCounter2.getYearByPeer(df)

    assert(result.map(_.toString) === expectedResult.map(_.toString))
  }
}
