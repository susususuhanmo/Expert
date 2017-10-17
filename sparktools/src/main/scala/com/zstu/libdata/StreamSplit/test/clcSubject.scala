package com.zstu.libdata.StreamSplit.test
import com.zstu.libdata.StreamSplit.function.CommonTools._
import com.zstu.libdata.StreamSplit.function.{ReadData, WriteData}
import com.zstu.libdata.StreamSplit.splitAuthor.getCLC
/**
  * Created by Administrator on 2017/7/31 0031.
  */
object clcSubject {
  def main(args: Array[String]): Unit = {
    val hiveContext = initSpark("clcSubject")
    val clcData  = ReadData.readDataLog("t_BookCLC",hiveContext)
      .withColumnRenamed("clc","classifications")
    val clcRdd = getCLC.getCLCRdd(hiveContext)
    val resultData = getCLC.addCLCName(clcData,clcRdd,hiveContext)
      .withColumnRenamed("classifications","clc")
    WriteData.writeDataLog("t_BookCLC_withSubject",resultData)

  }

}
