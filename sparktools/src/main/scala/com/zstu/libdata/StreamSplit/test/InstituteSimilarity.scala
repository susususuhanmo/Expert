package com.zstu.libdata.StreamSplit.test

import java.io.PrintWriter

import com.zstu.libdata.StreamSplit.function.{CommonTools, LevenshteinDistance, ReadData, WriteData}

/**
  * Created by Administrator on 2017/12/29 0029.
  */
object InstituteSimilarity {
  case class ResultRow(institute: String,instituteStd: String,similarity:Double)

  def isCnChar(ch: Char): Boolean = if(ch < 19968 || ch >19968+20901)  false else  true
  def isEnCHar(ch: Char) : Boolean = if((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')) true else false
  def isNum(ch: Char) : Boolean = if(ch >= '0' && ch <= '9') true else false

  def deletePunctuation(str: String): String ={
    var rtn = str
    if(rtn == null) return null
    rtn = ""
    var flag = false

    for(i <- 0 to str.length -1){
      val c = str(i)
      if(!isNum(c)) flag = true
      if((isCnChar(c)||isNum(c)) && flag)
        rtn += c
    }
    rtn
  }
  val logger = new PrintWriter("./InstituteSimilarity.txt")
  def main(args: Array[String]): Unit = {
//    println(deletePunctuation("123范德萨213"))
    val hiveContext = CommonTools.initSpark("InstituteSimilarity")
    val instituteData = ReadData.readData160("Institute","t_Institute",hiveContext).select("organization")
.map(r => (deletePunctuation(r.getString(r.fieldIndex("organization"))).substring(0,1)
  ,(deletePunctuation(r.getString(r.fieldIndex("organization"))),r.getString(r.fieldIndex("organization")))))
//    resultData.withColumn("partOrgan", resultData("firstLevelOrgan").substr(0,4))
    logger.println("读取完成" + instituteData.count())
    logger.flush()
////      .map(r => r.getString(r.fieldIndex("organization")))
    val instituteStdData= ReadData.readData160("Institute","t_StandardInstitute",hiveContext).select("name")
      .map(r => (deletePunctuation(r.getString(r.fieldIndex("name"))).substring(0,1)
        ,(deletePunctuation(r.getString(r.fieldIndex("name"))),r.getString(r.fieldIndex("name")))))
//      .map(r => r.getString(r.fieldIndex("name")))
    logger.println("读取完成" + instituteStdData.count())
    logger.flush()




    val joinedRdd = instituteData.join(instituteStdData).map(value => (value._2._1._1,value._2._2._1,value._2._1._2,value._2._2._2))


    logger.println("join完成" + joinedRdd.count())
    logger.flush()

//    val joinedRdd2 =  joinedRdd.map(r => (deletePunctuation(r.getString(r.fieldIndex("organization"))),
//      deletePunctuation(r.getString(r.fieldIndex("name"))),r.getString(r.fieldIndex("organization")),r.getString(r.fieldIndex("name"))))

    val similarityRdd = joinedRdd.map(value => (value._3,value._4,LevenshteinDistance.score(value._1,value._2)))
      .filter(value => value._3>=0.6).sortBy(value => value._3,false)
    logger.println("过滤排序完成" + similarityRdd.count())
    logger.flush()
    val similarityData = hiveContext
      .createDataFrame(similarityRdd.map(value => ResultRow(value._1,value._2,value._3)))
    logger.println("准备写入" + similarityData.count())
    logger.flush()
    WriteData.writeData160("Institute","t_InstituteSimilarity",similarityData)
  }

}
