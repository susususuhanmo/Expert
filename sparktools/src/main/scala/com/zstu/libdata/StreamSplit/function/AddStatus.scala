package com.zstu.libdata.StreamSplit.function

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by Administrator on 2017/10/23 0023.
  */
object AddStatus {
  def isCnChar(ch: Char): Boolean = if(ch < 19968 || ch >19968+20901)  false else  true
  def isEnCHar(ch: Char) : Boolean = if((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')) true else false
  def isValidInsittute(institute : String): Boolean ={
    if(institute == null || institute.length <4) return false
    for (c <- institute){
      if(!isCnChar(c) && !isEnCHar(c)) return false
    }
    true
  }
  def getStatus(institute : String): String = if(isValidInsittute(institute)) "0" else "98"



  def addStatus(authorData: DataFrame, hiveContext: HiveContext) = {
    hiveContext.udf.register("getStatus", (str: String) => if (str != "" && str != null) getStatus(str) else null)
authorData.registerTempTable("AddStatusAuthorData")
    val authorDataWithStatus = hiveContext.sql("select *,getStatus(organization) as status from AddStatusAuthorData")
    hiveContext.dropTempTable("AddStatusAuthorData")
    authorDataWithStatus
  }

  def main(args: Array[String]): Unit = {
    println(isEnCHar('a'))
  }
}
