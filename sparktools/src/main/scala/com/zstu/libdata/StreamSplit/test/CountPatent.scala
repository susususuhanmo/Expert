package com.zstu.libdata.StreamSplit.test

import com.zstu.libdata.StreamSplit.function.{CommonTools, ReadData, WriteData}

/**
  * Created by Administrator on 2018/3/20 0020.
  */
object CountPatent {

  case class Result(id : String, patentCounts: Int)
  def main(args: Array[String]): Unit = {
    val hiveContext = CommonTools.initSpark("CountPatent")
    val expertData  = ReadData.readData160("CERSv4","t_CandidateExpert",hiveContext).select("kid","name","organization")
      .map(r => (r.getString(r.fieldIndex("name"))+r.getString(r.fieldIndex("organization")),r.getString(r.fieldIndex("kid"))))
    val patentData  = ReadData.readData160("CERSv4","t_UnionResource",hiveContext)
      .filter("resourceCode='P' ")
      .select("creator","institute")
      .map(r => (r.getString(r.fieldIndex("creator"))+r.getString(r.fieldIndex("institute")),1))
    //(kid,authororg,)
    val matchedData = expertData.join(patentData).map(v => v._2).reduceByKey(_+_)
    val result = hiveContext.createDataFrame(matchedData.map(v => Result(v._1,v._2)))
    WriteData.writeData160("CERSv4","t_tmp_ExpertPatent",result)


  }

}
