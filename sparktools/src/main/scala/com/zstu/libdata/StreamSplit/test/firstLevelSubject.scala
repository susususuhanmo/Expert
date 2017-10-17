package com.zstu.libdata.StreamSplit.test

import com.zstu.libdata.StreamSplit.function.CommonTools.initSpark
import com.zstu.libdata.StreamSplit.function.{ReadData, WriteData}

/**
  * Created by Administrator on 2017/8/1 0001.
  */
object firstLevelSubject {
  def main(args: Array[String]): Unit = {
    val hiveContext = initSpark("firstLevelSubject")

    val subjectRdd = ReadData.readDataLog("t_CnkiSubject",hiveContext)
      .map(row =>
        (row.getString(row.fieldIndex("CNKI专题名称")),row.getString(row.fieldIndex("国标一级学科名称")))
      )

//      [t_ProjectSubject]
    val projectSubject = ReadData.readDataLog("t_ProjectSubject",hiveContext).withColumnRenamed("所属学科","subjects")

    val resultData = addFirstLevelSubject.addSubjectName(projectSubject,subjectRdd,hiveContext)
      .withColumnRenamed("subjects","所属学科").withColumnRenamed("firstLevelSubjects","国标一级学科名称")

  Thread.sleep(10000)
    WriteData.writeDataLog("t_ProjectSubject_WithFirstLevelSubject",resultData)


  }
}
