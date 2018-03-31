//package com.zstu.libdata.StreamSplit.test
//
//import java.io.PrintWriter
//
//import com.zstu.libdata.StreamSplit.function.CommonTools.trimStr
//import com.zstu.libdata.StreamSplit.function._
//import org.apache.spark.rdd.RDD
//import org.joda.time.DateTime
//
///**
//  * Created by Administrator on 2017/9/13 0013.
//  */
//object cleanUnion {
//  val logger = new PrintWriter("./cleanUnion.txt")
//  def logUtil(info: String): Unit = {
//    if (logger != null) {
//      logger.println(info +  DateTime.now +  "\r\n")
//      logger.flush()
//    }
//  }
//  def splitStr(str: String, mode: String = "cleaned", separator: String = ";"): Array[String] = {
//    if (str == null) return null
//    if (mode != "cleaned")
//      ReplaceLastStr.ReplaceLastStr(
//        str.trim.replace("；", separator)
//          .replace(",", separator)
//          .replace(";", separator)
//          .replace("，", separator)
//          .replace("|!", separator), separator)
//        .split(separator)
//        .map(trimStr)
//    else
//      str.split(separator).map(trimStr)
//  }
//
//  def clean(str: String): String = {
//    if (str == null) null
//    else {
//      val splitedStr = splitStr(deleteInvisibleChar.deleteInvisibleChar(str), mode = "clean")
//      splitedStr.reduce(_ + ";" + _)
//    }
//  }
//
//  case class columnName(id: String, keyword: String, keywordalt: String,
//                        creatorall: String,
//                        keywordnew: String, keywordaltnew: String,
//                        creatorallnew: String)
//
//  def main(args: Array[String]): Unit = {
//    val hiveContext = CommonTools.initSpark("cleanUnion")
//    //    id,keyword,keywordalt,creatorall
//    val unionData = ReadData.readDataCERSv4("t_UnionResource", hiveContext)
//      .select("id", "keyword", "keywordalt", "creatorall")
//    logUtil("读取完成" + unionData.count())
//    val unionRdd: RDD[(String, String, String, String)] = unionData.map(row => (
//      row.getString(row.fieldIndex("id")),
//      row.getString(row.fieldIndex("keyword")),
//      row.getString(row.fieldIndex("keywordalt")),
//      row.getString(row.fieldIndex("creatorall"))
//    )).filter(value =>
//      (value._2 != null && value._2 != "")
//        || (value._3 != null && value._3 != ""))
//    logUtil("过滤完成开始清洗" + unionRdd.count())
//    val cleanedRdd = unionRdd.map(value => columnName(
//        value._1,
//        value._2,
//        value._3,
//        value._4,
//        clean(value._2),
//        clean(value._3),
//        clean(value._4)
//      ))
//
//    val resultData = hiveContext.createDataFrame(cleanedRdd)
//    logUtil("清洗完成" + resultData.count())
//
//
//    //    unionData.registerTempTable("unionData")
//    //    hiveContext.udf.register("clean", (str: String) =>if(str !="" && str !=null) clean(str) else null)
//
//    //    val resultData= hiveContext.sql("select id,keyword,keywordalt,creatorall," +
//    //      "clean(keyword) as keywordnew," +
//    //      "clean(keywordalt) as keywordaltnew," +
//    //      "clean(creatorall) as creatorallnew " +
//    //       "from unionData").cache()
//
//
//    WriteData.writeDataLc("t_CleanedUnion1", resultData)
//
//
//  }
//}
