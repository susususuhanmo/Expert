package com.zstu.libdata.StreamSplit.test

import com.zstu.libdata.StreamSplit.function.WriteData.writeDataLog196
import com.zstu.libdata.StreamSplit.function.{CommonTools, ReadData}
import com.zstu.libdata.StreamSplit.kafka.{cnkiClean, commonClean}
import com.zstu.libdata.StreamSplit.splitAuthor.getCLC.{addCLCName, getCLCRdd}
import com.zstu.libdata.StreamSplit.splitAuthor.splitAuthorFunction.{keyWordData, keyWordSplitNew}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by Administrator on 2018/1/9 0009.
  */
object keywordSplit {
  def main(args: Array[String]): Unit = {
    val hiveContext = CommonTools.initSpark("split")
    val CLCRdd: RDD[(String, (String, String))] = getCLCRdd(hiveContext)
    val orgData = commonClean.readDataOrg("t_CNKI_UPDATE", hiveContext)


    val inputData = addCLCName(orgData
      //      .drop("code").drop("subject")
      .filter("year = 2017").withColumnRenamed("subject", "classifications"), CLCRdd, hiveContext)
      .map(row =>
        (
          row.getString(row.fieldIndex("GUID")),
          (
            cnkiClean.cleanKeyWord(row.getString(row.fieldIndex("keyword"))),
            null,
            row.getString(row.fieldIndex("subject"))
          )
        ))


    val keyWord = inputData.flatMap(keyWordSplitNew).filter(f => f != null)
      .map(value =>
        keyWordData(value._1, value._2, "2017", CommonTools.cutStr(value._4, 500), CommonTools.cutStr(value._5, 500), value._6))

    //    case class keyWordData(subjectCode: String, paperid: String, year: String, keyword: String, keywordAlt: String
    //                           , subjectName: String)
    val keyWordResultData = hiveContext.createDataFrame(keyWord)

    writeDataLog196("t_KeywordLog_all", keyWordResultData)
    //    (paperId, (keywordNew, keywordAltNew, subjectNew))
  }

  def keywordSplitAll(orgData: DataFrame,hiveContext: HiveContext) = {

    val inputData = orgData.withColumnRenamed("id","GUID")
      //      .drop("code").drop("subject")
      .map(row =>
        (
          row.getString(row.fieldIndex("GUID")),
          (
            cnkiClean.cleanKeyWord(row.getString(row.fieldIndex("keyWord"))),
            cnkiClean.cleanKeyWord(row.getString(row.fieldIndex("keyWordAlt"))),
            row.getString(row.fieldIndex("subject"))
          )
        ))
  val yearData = orgData.select("GUID","year")

    val keyWord = inputData.flatMap(keyWordSplitNew).filter(f => f != null)
      .map(value =>
        keyWordData(value._1, value._2, null, CommonTools.cutStr(value._4, 500), CommonTools.cutStr(value._5, 500), value._6))

    //    case class keyWordData(subjectCode: String, paperid: String, year: String, keyword: String, keywordAlt: String
    //                           , subjectName: String)
    val keyWordResultData = hiveContext.createDataFrame(keyWord).drop("year")
val resultData = keyWordResultData.join(yearData,keyWordResultData("paperid") === yearData("GUID")).drop("GUID")
    writeDataLog196("t_KeywordLog", resultData)
  }
}
