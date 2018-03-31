package com.zstu.libdata.StreamSplit.test

import com.zstu.libdata.StreamSplit.function.{CommonTools, ReadData, WriteData}
import org.apache.spark.rdd.RDD

/**
  * Created by Administrator on 2018/1/8 0008.
  */
object KeywordReduce {

  case class result(keyword:String,year_count:String,totalcounts:Int)
  def main(args: Array[String]): Unit = {
    val hiveContext = CommonTools.initSpark("KeywordReduce")
    val keyWordData = ReadData.readData160("FzjV3","t_Keyword_sat1",hiveContext)
      .map(r => (r.getString(r.fieldIndex("keyword")),
        (
          r.getString(r.fieldIndex("YEAR")) + ":"+r.getString(r.fieldIndex("counts"))
        ,r.getString(r.fieldIndex("counts")).toInt
        )
      ))
  val resultRdd: RDD[(String, (String, Int))] = keyWordData.reduceByKey((value1, value2)
    => {
    val (yearCounts1,counts1)= value1
    val (yearCounts2,counts2) = value2
    (yearCounts1 + ";" + yearCounts2,counts1+counts2)
  })
    val resultData = hiveContext.createDataFrame(resultRdd.map(value => result(value._1,value._2._1,value._2._2)))
    WriteData.writeData160("FzjV3","t_Keyword_Grouped",resultData)



  }
}
