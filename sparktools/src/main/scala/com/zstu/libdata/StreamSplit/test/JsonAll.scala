package com.zstu.libdata.StreamSplit.test

import java.io.PrintWriter

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by Administrator on 2017/11/3 0003.
  */
object JsonAll {
  val conf = new SparkConf().setMaster("local").setAppName("testSpark")
  val sc = new SparkContext(conf)
  val logger = new PrintWriter("./Json2Sqlserver.txt")
  val hiveContext = new HiveContext(sc)

  def main(args: Array[String]): Unit = {
//    try {
//      Json2Sqlserver.parseFile("file:///root/aminer_papers_" +"3" + ".txt", sc, hiveContext, logger)
//    } catch {
//      case e: Exception => {
//        logger.println(e.getMessage)
//        logger.flush()
//      }
//    }

    for(i<- 30 to 58)
    {
      try{
        Json2Sqlserver.parseFile("file:///data/aminer/aminer_papers_1/aminer_papers_"+i+".txt",sc,hiveContext,logger,i.toString)
      }catch{
        case e:Exception => {
          logger.println(e.getMessage)
          logger.flush()
        }
      }
    }
  }


}
