package com.zstu.libdata.StreamSplit.test

import java.io.PrintWriter

import com.zstu.libdata.StreamSplit.function.{ReadData, WriteData}
import com.zstu.libdata.StreamSplit.test.DealJsonString.parseAuthors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/11/1 0001.
  */
object Json2Sqlserver {

  def parseFile(fileName:String,sc:SparkContext,hiveContext: HiveContext,logger:PrintWriter,num:String): Unit = {
    val sampleData: RDD[String] = sc.textFile(fileName)
    logger.println("第"+num+"页数据读取完毕" + sampleData.count())
    logger.println(sampleData.first())
    logger.flush()
val parsedData: RDD[DealJsonString.Aminer] = sampleData.map(jsonString => DealJsonString.dealJsonString(jsonString))
    logger.println("解析完毕" + parsedData.count())
    logger.flush()

    hiveContext.createDataFrame(parsedData).registerTempTable("parsedData")
    hiveContext.udf.register("getAuthorName", (str: String) => if (str != "" && str != null) parseAuthors(str)._1 else null)
    hiveContext.udf.register("getAuthorOrg", (str: String) => if (str != "" && str != null) parseAuthors(str)._2 else null)
val resultData= hiveContext.sql("select *,getAuthorName(authors) as authorName,getAuthorOrg(authors) as authorOrg,"+num+" as num from parsedData")

    logger.println("作者解析完毕" + resultData.count())
    logger.println(resultData.first())
    logger.flush()





    WriteData.writeData50("Aminer","t_aminer_papers",resultData)

      logger.println("第"+num+"页写入完成")
    logger.flush()
  }


}
