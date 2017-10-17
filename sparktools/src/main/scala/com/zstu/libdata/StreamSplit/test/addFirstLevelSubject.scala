package com.zstu.libdata.StreamSplit.test

import com.zstu.libdata.StreamSplit.function.CommonTools
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by Administrator on 2017/8/1 0001.
  */
object addFirstLevelSubject {


  case class columnName(subjectsForJoin: String,cleanedSubjects: String,firstLevelSubjects: String)
def toFirstLevel(subject: String,subjectArray:Array[(String,String)]):String = {
  if(subject == null) return null

  for(firstLevel <- subjectArray){
//    println(subject + "---------" +firstLevel._1 )
    if(subject == firstLevel._1) {

//      println(firstLevel._1)
      return firstLevel._2
    }
  }
  null




}

  def getFirstLevelSubjects(subjects : String,subjectArray:Array[(String,String)]):(String,String) ={


    if(subjects == null)  return null

    var subjectsCleaned =subjects
    if(subjectsCleaned(subjectsCleaned.length - 1) == ';'){
      subjectsCleaned = subjectsCleaned.substring(0,subjectsCleaned.length -1)
    }
    val subjectsArray = subjectsCleaned.split(';').map(value => CommonTools.cutStr(value,4)).distinct




    val subjectsAndFirstLevel1 = for(subject <- subjectsArray) yield toFirstLevel(subject,subjectArray)
//    subjectsAndFirstLevel1.foreach(println)

    val subjectsAndFirstLevel = subjectsAndFirstLevel1.filter(firstLevel => firstLevel  != null )



    if (subjectsAndFirstLevel.isEmpty) {

      return (subjectsCleaned,null)
    }
    val firstLevelSubjects = subjectsAndFirstLevel.distinct.reduce(_ + ";" + _)

    (subjectsCleaned,firstLevelSubjects)
  }


  /**
    *
    * @param inputData 要求有sujects列。
    * @param subjectRdd 学科和一级学科对照关系(学科，一级学科)
    * @param hiveContext
    * @return 增加firstLevelSubjects列(subjects: String,firstLevelSubjects: String)
    */
  def addSubjectName(inputData: DataFrame, subjectRdd: RDD[(String,String)], hiveContext: HiveContext)
  : DataFrame = {
//    获取去重后的所有学科名称组合

//(subject,firstlevel)
    val subjectArray = subjectRdd.collect()


    val inputSubjectRdd = inputData.map(row => row.getString(row.fieldIndex("subjects"))).distinct()

//    (subjectsForJoin: String,cleanedSubjects: String,firstLevelSubjects: String)

    val subjectsCleanedFirst = inputSubjectRdd.map(value => {
      val cleanAndFirst =getFirstLevelSubjects(value,subjectArray)
      if(cleanAndFirst == null ) null
      else
      columnName(value,value,cleanAndFirst._2)
    }).filter(value => value != null)

//    (subjectsForJoin: String,cleanedSubjects: String,firstLevelSubjects: String)
    val subjectsCleanedFirstData = hiveContext.createDataFrame(subjectsCleanedFirst)
val resultData = inputData.join(subjectsCleanedFirstData,
  inputData("subjects") === subjectsCleanedFirstData("subjectsForJoin"),"left"
).drop("subjects").drop("subjectsForJoin").withColumnRenamed("cleanedSubjects","subjects")

    resultData


  }

}
