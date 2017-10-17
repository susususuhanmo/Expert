package com.zstu.libdata.StreamSplit

import com.zstu.libdata.StreamSplit.function.getData.{getForSplitRdd, getRightRddAndReportError, readSourceRdd}
import com.zstu.libdata.StreamSplit.function.newDataOps.dealNewData0623
import com.zstu.libdata.StreamSplit.function._
import com.zstu.libdata.StreamSplit.kafka.commonClean
import com.zstu.libdata.StreamSplit.splitAuthor.getCLC.addCLCName
import com.zstu.libdata.StreamSplit.function.printLog.logUtil
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by Administrator on 2017/6/24 0024.
  */
object mainWF {


  def main(hiveContext:HiveContext): Unit = {

    val types = 8
    var (clcRdd, authorRdd, simplifiedJournalRdd, journalMagSourceRdd, universityData)
    = readSourceRdd(hiveContext)


    logUtil("数据读取完成")
    logUtil("clc" + clcRdd.count())
    logUtil("author" + authorRdd.count())
    logUtil("simpified" + simplifiedJournalRdd.count())
    logUtil("Mag" + journalMagSourceRdd.count())
    //    while(true)
    //      {
    try {


      val orgjournaldata = commonClean.readDataOrg("t_WF_UPDATE", hiveContext)
        .filter("status != 1 and status != 3 and year =2017").limit(50000).cache()
      orgjournaldata.registerTempTable("t_orgjournaldataWF")




      logUtil("全部数据" + orgjournaldata.count())

      val (simplifiedInputRdd,repeatedRdd) =
        distinctRdd.distinctInputRdd(orgjournaldata.map(f => commonClean.transformRdd_wf_simplify(f)))
      logUtil("简化后的数据" + simplifiedInputRdd.count())


      WriteData.writeErrorData(repeatedRdd,types,hiveContext)
      logUtil("重复数据写入" + repeatedRdd.count())
      val removePostCode = new RemovePostCode(hiveContext)
      val fullInputData = addCLCName(getData.getFullDataWFsql(hiveContext,removePostCode),clcRdd,hiveContext)







      val forSplitRdd = getForSplitRdd(fullInputData,removePostCode)
      logUtil("待拆分的数据" + forSplitRdd.count())





      //过滤出正常数据并将错误数据反馈
      val (rightInputRdd,errorRdd) = getRightRddAndReportError(simplifiedInputRdd, hiveContext)
      logUtil("正常数据" + rightInputRdd.count())

      WriteData.writeErrorData(errorRdd,types,hiveContext)




      //开始查重 join group
      val inputJoinJournalRdd = rightInputRdd.leftOuterJoin(simplifiedJournalRdd).map(f => (f._2._1._4, f._2))
      logUtil("join成功" + inputJoinJournalRdd.count())



      //处理新数据 得到新的journal大表 和 新作者表
      val newAuthorRdd =
        dealNewData0623( fullInputData
          , journalMagSourceRdd, simplifiedJournalRdd, types,inputJoinJournalRdd
          , authorRdd, clcRdd, hiveContext, forSplitRdd, universityData)
      logUtil("新数据处理成功获得新数据")


      authorRdd = newAuthorRdd
      logUtil("数据更新成功")


      //处理旧数据


      val num = oldDataOps.dealOldData(fullInputData, types, inputJoinJournalRdd
        , hiveContext)
      logUtil("匹配成功的旧数据处理成功" + num)
      logUtil("---------")
      val logData = hiveContext.sql("select GUID as id,"+types+" as resource from t_orgjournaldataWF")
      WriteData.writeDataWangzhihong("t_Log",logData)
      hiveContext.dropTempTable("t_orgjournaldataWF")
    } catch {
      case ex: Exception => logUtil(ex.getMessage)
    }


  }
}
