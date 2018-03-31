package com.zstu.libdata.StreamSplit

import com.zstu.libdata.StreamSplit.function.getData.{getForSplitRdd, getRightRddAndReportError, readSourceRdd}
import com.zstu.libdata.StreamSplit.function.newDataOps.dealNewData0623
import com.zstu.libdata.StreamSplit.function._
import com.zstu.libdata.StreamSplit.test.keywordSplit
import org.apache.spark.sql.hive.HiveContext

//import com.zstu.libdata.StreamSplit.function.oldDataOps.dealOldData
import com.zstu.libdata.StreamSplit.kafka.commonClean
import com.zstu.libdata.StreamSplit.splitAuthor.getCLC.addCLCName
import com.zstu.libdata.StreamSplit.function.printLog.logUtil

/**
  * Created by Administrator on 2017/6/24 0024.
  */
object mainVIP {

  def main(hiveContext:HiveContext): Unit = {




    //    while(true)
    //      {
    try
    {

      //      val CNKIData = readData165("t_CNKI_UPDATE",hiveContext).limit(3000)
      //    (key, (title, journal, creator, id, institute,year))

      val orgjournaldata = commonClean.readDataOrg("t_VIP_UPDATE", hiveContext)
        .filter("status!=2&&status!=6&&status!=10&&status!=14").limit(50000).cache()

      orgjournaldata.registerTempTable("t_orgjournaldataVIP")
      val yearFilter = "(" + hiveContext.sql("select distinct year from t_orgjournaldataVIP")
        .map(r => "year = " + r.getString(r.fieldIndex("year"))).collect().reduce(_ + " or "+ _) + ")"

      val types = 4
      var (clcRdd, authorRdd, simplifiedJournalRdd,universityData)
      = readSourceRdd(hiveContext,yearFilter)


      logUtil("数据读取完成")
      logUtil("clc" + clcRdd.count())
      logUtil("author" + authorRdd.count())
      logUtil("simpified" + simplifiedJournalRdd.count())

      val postArray = getData.getPostArray(hiveContext)
      val removePostCode = new RemovePostCode(hiveContext,postArray)
      val fullInputData=  addCLCName(getData.getFullDataVIPsql(hiveContext,removePostCode),clcRdd,hiveContext)
      //对所有数据的关键词进行拆分和写入
      keywordSplit.keywordSplitAll(fullInputData,hiveContext)


      val (simplifiedInputRdd,repeatedRdd) =
        distinctRdd.distinctInputRdd(orgjournaldata.map(f =>commonOps.transformRdd_vip_simplify(f)))


      WriteData.writeErrorData(repeatedRdd,types,hiveContext)
      logUtil("重复数据写入" + repeatedRdd.count())



      // val simplifiedInputRdd =getSimplifiedInputRdd(CNKIData)
      logUtil("简化后的数据" + simplifiedInputRdd.count())
      val forSplitRdd =getForSplitRdd(fullInputData,removePostCode)
      logUtil("待拆分的数据" + forSplitRdd.count())

      //过滤出正常数据并将错误数据反馈
      val (rightInputRdd,errorRdd) = getRightRddAndReportError(simplifiedInputRdd, hiveContext)
      logUtil("正常数据" + rightInputRdd.count())

      WriteData.writeErrorData(errorRdd,types,hiveContext)


      //开始查重 join group
      val inputJoinJournalRdd = rightInputRdd.leftOuterJoin(simplifiedJournalRdd).map(f => (f._2._1._4, f._2))
      logUtil("join成功" + inputJoinJournalRdd.count())
      val joinedGroupedRdd = inputJoinJournalRdd.groupByKey()
      logUtil("group成功" + joinedGroupedRdd.count())


      val newAuthorRdd =
        dealNewData0623( fullInputData
          , simplifiedJournalRdd, types,inputJoinJournalRdd
          , authorRdd, clcRdd, hiveContext, forSplitRdd, universityData)
      logUtil("新数据处理成功获得新数据")


      authorRdd = newAuthorRdd
      logUtil("数据更新成功")


      //处理旧数据
//      val num = dealOldData(inputJoinJournalRdd, fullInputRdd, sourceCoreRdd
//        , journalMagSourceRdd, simplifiedJournalRdd, types)
      val num = oldDataOps.dealOldData(fullInputData, types, inputJoinJournalRdd
        , hiveContext)
      logUtil("匹配成功的旧数据处理成功" + num)
      val logData = hiveContext.sql("select GUID as id,"+types+" as resource from t_orgjournaldataVIP")
      WriteData.writeDataWangzhihong("t_Log196",logData)
      hiveContext.dropTempTable("t_orgjournaldataVIP")
    }catch {
      case ex: Exception => logUtil(ex.getMessage)
    }




  }
}
