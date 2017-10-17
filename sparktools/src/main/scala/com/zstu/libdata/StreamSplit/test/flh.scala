package com.zstu.libdata.StreamSplit.test
import com.zstu.libdata.StreamSplit.function.CommonTools._
import com.zstu.libdata.StreamSplit.function.{CommonTools, ReadData, WriteData}

/**
  * Created by Administrator on 2017/8/8 0008.
  */
object flh {

  def main(args: Array[String]): Unit = {
    val hiveContext = initSpark("firstLevelSubject")

    val subjectRdd = ReadData.readDataLog("专利与国标一级学科对照表$",hiveContext)
      .map(row =>
        (CommonTools.cutStr(row.getString(row.fieldIndex("4级类号")).trim,4),row.getString(row.fieldIndex("国标一级学科类名")))
      )

    //      [t_ProjectSubject]
    val projectSubject = ReadData.readDataLog("t_PatentFlh",hiveContext).withColumnRenamed("flh","subjects")

    val resultData = addFirstLevelSubject.addSubjectName(projectSubject,subjectRdd,hiveContext)
      .withColumnRenamed("subjects","flh").withColumnRenamed("firstLevelSubjects","国标一级学科类名")

    Thread.sleep(10000)
    WriteData.writeDataLog("t_PatentFlh_国标",resultData)


  }

}
