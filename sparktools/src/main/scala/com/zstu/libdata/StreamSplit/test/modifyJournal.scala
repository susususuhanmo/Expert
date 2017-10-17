package com.zstu.libdata.StreamSplit.test


import java.io.PrintWriter

import com.zstu.libdata.StreamSplit.function.deleteZero
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.joda.time.DateTime

/**
  * Created by Administrator on 2017/9/29 0029.
  */
object modifyJournal {
  val logger = new PrintWriter("./modify.txt")


  def modify(allColumCoreData: DataFrame, originJournalData: DataFrame, hiveContext: HiveContext): DataFrame = {
    originJournalData.drop("issn").drop("datasource").drop("isCore").registerTempTable("originJournal")

    val coreData = allColumCoreData.select("CNKIKEY", "VIPKEY", "WFKEY", "ID", "ISSN", "DATASOURCE")
      .withColumnRenamed("ID", "journalCoreId")

    logger.println("coreData" + coreData.count() + DateTime.now + "\r\n")
    logger.flush()
    hiveContext.udf.register("deleteZero", (str: String) => if (str != "" && str != null) deleteZero.deleteZero(str) else null)

    hiveContext.udf.register("getKey", (url: String, resource: String) => if (url != "" && url != null) ParseUrl.GetKey(url, resource) else null)
    hiveContext.udf.register("parseResource", (url: String) => ParseUrl.ParseResource(url))
    val parsedJournalData = hiveContext.sql("select id,title,titleAlt,creator,creatorAll,institute,instituteAll,abstract,abstractAlt,keywords,keywordAlt,classifications,language,page,doi,journal,year,deleteZero(volume) as volume,deleteZero(issue) as issue,resource,defaultUrl,resourceDetail," +
      "parseResource(defaultUrl) as journalResource," +
      "getKey(defaultUrl,parseResource(defaultUrl)) as key," +
      "1 as isCore " +
      "from originJournal")
//    logger.println("解析完毕" + parsedJournalData.first())
//
//    logger.flush()

//        val wfResultData   =matchData(parsedJournalData,coreData,"wf",hiveContext)
        val cnkiResultData =matchData(parsedJournalData,coreData,"cnki",hiveContext)
        val vipResultData  =matchData(parsedJournalData,coreData,"vip",hiveContext)
    //
    logger.println("回到主程序")

    logger.flush()
//    wfResultData
    cnkiResultData unionAll vipResultData
  }

  def matchData(originData: DataFrame, allCoreData: DataFrame, resource: String, hiveContext: HiveContext): DataFrame = {

    val filteredData = originData.filter("journalResource='" + resource + "'").drop("journalResource")

    logger.println(resource + "Data" + filteredData.count() + DateTime.now + "\r\n")
    logger.flush()

    val coreData =
      resource match {
        case "wf" => allCoreData.drop("VIPKEY").drop("CNKIKEY").filter("WFKEY is not null")
        case "vip" => allCoreData.drop("WFKEY").drop("CNKIKEY").filter("VIPKEY is not null")
        case "cnki" => allCoreData.drop("WFKEY").drop("VIPKEY").filter("CNKIKEY is not null")
      }
    resource match {
      case "wf" => filteredData.join(coreData, filteredData("key") === coreData("WFKEY")).drop("WFKEY").drop("key")
      case "vip" => filteredData.join(coreData, filteredData("key") === coreData("VIPKEY")).drop("VIPKEY").drop("key")
      case "cnki" => filteredData.join(coreData, filteredData("key") === coreData("CNKIKEY")).drop("CNKIKEY").drop("key")
    }
  }

}
