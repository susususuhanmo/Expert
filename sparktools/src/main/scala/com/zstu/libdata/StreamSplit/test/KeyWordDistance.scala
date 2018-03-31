package com.zstu.libdata.StreamSplit.test

import com.zstu.libdata.StreamSplit.function.{CommonTools, ReadData, WriteData}

/**
  * Created by Administrator on 2018/1/25 0025.
  */
object KeyWordDistance {
  case class result(paperId: String,keyword:String)
  def main(args: Array[String]): Unit = {
    val hiveContext = CommonTools.initSpark("")
    val keyWordData = ReadData.readData160("FzjV3", "t_Keyword_new", hiveContext)
    hiveContext.udf.register("len", (str: String) => if (str != "" && str != null)str.length else 0)
    val simpleKeyWordData = keyWordData.select("paperId","keyword").filter("len(keyword)>0").orderBy("paperId").limit(10000)
    val simpleKeyWordRdd = simpleKeyWordData.map(r => (r.getString(r.fieldIndex("paperId")),r.getString(r.fieldIndex("keyword"))))
      .reduceByKey(_+";"+_)
    val resultData = hiveContext.createDataFrame(simpleKeyWordRdd.map(value => result(value._1,value._2)))
    WriteData.writeData50("Crawler","t_keyword_simple",resultData)
  }
}
