package com.zstu.libdata.StreamSplit.test

import com.zstu.libdata.StreamSplit.function.ReadData.readDataLog
import com.zstu.libdata.StreamSplit.function.{CommonTools, ReadData, WriteData, cnkiOps}

/**
  * Created by Administrator on 2017/7/17 0017.
  */
object coreCheck {
  def main(args: Array[String]): Unit = {
    val hiveContext = CommonTools.initSpark("checkCore")
    val sourceCoreRdd = readDataLog("t_JournalCore", hiveContext)
      .map(f => (f.getString(f.fieldIndex("name")),f.getString(f.fieldIndex("issn"))))


    val unionData = ReadData.readDataCERSv4("t_UnionResource",hiveContext)
        .filter("resourceCode = 'J'")
      .select("id","isCore","journal","issn")
    unionData.registerTempTable("unionData")
    val coreJournals = sourceCoreRdd.collect()



    def isCore(journal: String,issn: String) : Boolean ={
      if(journal == null) false
      else {
        for(coreJournalInfo <- coreJournals){
          val coreJournal = coreJournalInfo._1
          val coreIssn = coreJournalInfo._2

          if(journal == coreJournal
          ) return true
          if(issn!= null && coreIssn != null && issn == coreIssn)
            return true
        }
        false
      }
    }
    hiveContext.udf.register("checkCore", (str: String,issn:String) =>isCore(str,issn) )
    hiveContext.udf.register("cleanJournal", (str: String) =>if(str !="" && str !=null) cnkiOps.cleanJournal(str) else null)

    val resultData = hiveContext.sql("select id,issn,isCore as isCoreOld" +
      ",cleanJournal(journal) as journal" +
      ",checkCore(cleanJournal(journal),issn) as isCore from unionData")
    WriteData.writeDataLog("t_isCoreCheck",resultData)



  }
}
