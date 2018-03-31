package com.zstu.libdata.StreamSplit.test

import com.zstu.libdata.StreamSplit.function.{CommonTools, ReadData, WriteData}
import org.apache.spark.rdd.RDD

/**
  * Created by Administrator on 2018/1/8 0008.
  */
object KeywordReduceClean {
  case class result(keyword:String,year_count:String,totalcounts:Int)
  def isCnChar(ch: Char): Boolean = if(ch < 19968 || ch >19968+20901)  false else  true
  def isEnCHar(ch: Char) : Boolean = if((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')) true else false
  def isNum(ch: Char) : Boolean = if(ch >= '0' && ch <= '9') true else false

  def deletePunctuation(str: String): String ={
    var rtn = str
    if(rtn == null) return null
    rtn = ""


    for(i <- 0 to str.length -1){
      val c = str(i)

      if(isCnChar(c)||isNum(c)||isEnCHar(c))
        rtn += c
    }
    rtn
  }
  def main(args: Array[String]): Unit = {
    val hiveContext = CommonTools.initSpark("KeywordReduce")
    val keyWordData = ReadData.readData160("FzjV3","t_Keyword_sat1",hiveContext)
      .map(r => ((deletePunctuation(r.getString(r.fieldIndex("keyword"))),
          r.getString(r.fieldIndex("YEAR")))
        ,r.getString(r.fieldIndex("counts")).toInt
        )
      )
    val cleanedKeyWordData = keyWordData.reduceByKey((count1,count2) => count1+count2)
      .map(value => (value._1._1,(value._1._2 + ":" + value._2,value._2)))

    val resultRdd: RDD[(String, (String, Int))] = cleanedKeyWordData.reduceByKey((value1, value2)
    => {
      val (yearCounts1,counts1)= value1
      val (yearCounts2,counts2) = value2
      (yearCounts1 + ";" + yearCounts2,counts1+counts2)
    })
    val resultData = hiveContext.createDataFrame(resultRdd.map(value => result(value._1,value._2._1,value._2._2)))
    WriteData.writeData160("FzjV3","t_Keyword_Grouped_Cleaned",resultData)



  }
}
