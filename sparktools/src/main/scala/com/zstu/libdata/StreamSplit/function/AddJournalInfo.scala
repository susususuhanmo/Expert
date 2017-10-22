package com.zstu.libdata.StreamSplit.function

import java.io.PrintWriter

import com.zstu.libdata.StreamSplit.test.ParseUrl
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.joda.time.DateTime

/**
  * Created by Administrator on 2017/9/29 0029.
  */
object AddJournalInfo {
  val logger = new PrintWriter("./DiscoveryModify.txt")

  /**
    *
    * @param allColumCoreData  核心数据 要求有"CNKIKEY", "VIPKEY", "WFKEY", "ID", "ISSN", "DATAS这OURCE" 五个字段
    * @param originJournalData
    *                          drop("issn").drop("datasource").drop("isCore") 原始数据需要没有待添加的三个字段
    *                          且有defaultUrl字段
    * @param hiveContext
    * @return
    */

  def addCoreIssnDatasource(allColumCoreData: DataFrame, originJournalData: DataFrame, hiveContext: HiveContext,resource:String): DataFrame = {
    originJournalData.registerTempTable("originJournal")

    val coreData = allColumCoreData.select(resource.toUpperCase +"KEY", "ISSN", "DATASOURCE").withColumnRenamed(resource.toUpperCase+ "KEY","resourceKey")
//      .withColumnRenamed("ID", "journalCoreId")

    logger.println("coreData" + coreData.count() + DateTime.now + "\r\n")
    logger.flush()
    hiveContext.udf.register("deleteZero", (str: String) => if (str != "" && str != null) deleteZero.deleteZero(str) else null)

    hiveContext.udf.register("getKey", (url: String, resource: String) => if (url != "" && url != null) ParseUrl.GetKey(url, resource) else null)
    hiveContext.udf.register("parseResource", (url: String) => ParseUrl.ParseResource(url))
    val parsedJournalData = hiveContext.sql("select *," +
      "getKey(url,'"+ resource +"') as key " +

      "from originJournal")
    matchData(parsedJournalData,coreData,resource,hiveContext)
  }

  def matchData(originData: DataFrame, allCoreData: DataFrame, resource: String, hiveContext: HiveContext): DataFrame = {

    val filteredData = originData
    logger.println(resource + "Data" + filteredData.count() + DateTime.now + "\r\n")
    logger.flush()



    val coreData = allCoreData.filter("resourceKey is not null")


    dealColumn(filteredData.join(coreData, filteredData("key") === coreData("resourceKey"),"left").drop("key"),hiveContext)


  }
  def dealColumn (inputData : DataFrame,hiveContext: HiveContext)={
    inputData.registerTempTable("dealColumnData")
    hiveContext.udf.register("judgeCore", (key: String) =>if(key != null) 1 else 0)
    hiveContext.sql("select *,judgeCore(resourceKey) as isCore from dealColumnData").drop("resourceKey")
  }

}
