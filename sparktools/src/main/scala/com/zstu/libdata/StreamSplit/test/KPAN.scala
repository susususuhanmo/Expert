package com.zstu.libdata.StreamSplit.test

import com.zstu.libdata.StreamSplit.function.{CommonTools, ReadData, WriteData}

/**
  * Created by Administrator on 2017/7/25 0025.
  */
object KPAN {
  def main(args: Array[String]): Unit = {
    val hiveContext = CommonTools.initSpark("KPAN")

    val PANdata = ReadData.readDataV3("t_PaperAuthor_kid",hiveContext)
      .select("paperId","authorId","name")
    val KPdata = ReadData.readDataV3("t_Keyword_new",hiveContext)
      .select("paperid","keyword")
      .withColumnRenamed("paperId","paperIdK")
    val resultData = PANdata.join(KPdata,PANdata("paperId") === KPdata("paperIdK"))
      .drop("paperIdK")

    WriteData.writeDataLog("t_PaperAuthorKeyword",resultData)
  }

}
