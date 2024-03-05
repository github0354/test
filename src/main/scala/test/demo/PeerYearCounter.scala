package test.demo

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

/**
 * 测试
 */
object PeerYearCounter {

  case class Result(peerId: String, year: Int)

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder()
      .appName("Peer Year Counter")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

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

//    val data = Seq(
//      ("AE686(AE)", "7", "AE686", 2022),
//    ("AE686(AE)", "8", "BH2740", 2021),
//    ("AE686(AE)", "9", "EG999", 2021),
//    ("AE686(AE)", "10", "AE0908", 2023),
//    ("AE686(AE)", "11", "QA402", 2022),
//    ("AE686(AE)", "12", "OA691", 2022),
//    ("AE686(AE)", "12", "OB691", 2022),
//    ("AE686(AE)", "12", "OC691", 2019),
//    ("AE686(AE)", "12", "OD691", 2017)
//
//    )

    val df = data.toDF("peer_id", "id_1", "id_2", "year")


    //Step 1 : 对于每个peer_id，获取peer_id包含id_2时的年份
    val yearByPeer = df.withColumn("contains_id_2", expr("instr(peer_id, id_2) > 0")).filter(expr("contains_id_2 = true"))
      .withColumnRenamed("year", "min_year")


    yearByPeer.show()


    //Step 2 : 给定一个大小数，例如3。对于每个peer_id，计算每个年份(小于或等于step1中的年份)的个数
      val countsByYear = df.join(yearByPeer, Seq("peer_id"), "inner")
      .filter(expr("year <= min_year"))
      .groupBy("peer_id", "year")
      .count()

    countsByYear.show()
    countsByYear.orderBy("year").show()

    // Step 3 : 将步骤2中的值按年份排序，并检查第一年的计数数是否大于或等于给定的大小数。如果是，只需返回年份。如果不是，则加上从最大年份到下一年的计数数，直到计数数大于或等于给定的数

    // Given size number
    val givenSize = 3
    // 按年升序排序数据
    val sortedData = countsByYear.sort(desc("year"))
    // 按peer_id分组，以列表形式收集年份和计数
    val result = sortedData.groupBy("peer_id").agg(collect_list(struct("year", "count")).as("data"))

    val output = result.flatMap { row =>
      val peer_id = row.getString(0)
      val dataList = row.getAs[Seq[Row]](1)

      var cumulativeCount = 0
      var outputList = List.empty[Result]
      // 变量来控制循环
      var stop = false

      //使用stop变量来控制循环
      for (data <- dataList if !stop) {
        val year = data.getInt(0)
        val count = data.getLong(1).toInt
        cumulativeCount += count
        //将Result对象添加到outputList中
        outputList = Result(peer_id, year) :: outputList
        if (cumulativeCount >= givenSize) {
          //将stop设置为true以中断循环
          stop = true
        }
      }

      outputList.reverse
    }

    output.toDF().show(false)

    spark.stop()

  }


}
