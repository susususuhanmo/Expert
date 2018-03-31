package com.zstu.libdata.StreamSplit.test.ML

import java.io.PrintWriter

import com.zstu.libdata.StreamSplit.function.{CommonTools, ReadData}
import org.apache.spark.rdd.RDD

/**
  * Created by Administrator on 2017/11/21 0021.
  */
object SamePersonLearning {
  val logger = new PrintWriter("./DealJson.txt")
  def main(args: Array[String]): Unit = {
    val hiveContext = CommonTools.initSpark("SamePersonTrain")
    val paperData = ReadData.readData50("MachineLearning", "t_ExpertResource", hiveContext)
      .select("kid",
        "creatorall",
        "subject",
        "province",
        "address",
        "institute",
        "journal"
      )

    val expertData = ReadData.readData50("MachineLearning", "t_Expert", hiveContext)
      .select("Ekid", "name")
    //      .map(r => (r.getString(r.fieldIndex("kid")), r.getString(r.fieldIndex("name"))))

    val joinedData = expertData.join(paperData, expertData("Ekid") === paperData("kid")).drop("Ekid")

    val nameGroupData: RDD[(String, Iterable[(String, Person)])] = expertData
      .map(r => (r.getString(r.fieldIndex("kid")),
        r.getString(r.fieldIndex("name")),
        r.getString(r.fieldIndex("creatorall")),
        r.getString(r.fieldIndex("subject")),
        r.getString(r.fieldIndex("province")),
        r.getString(r.fieldIndex("address")),
        r.getString(r.fieldIndex("institute")),
        r.getString(r.fieldIndex("journal"))))
        .map(value => (value._2,(value._1,formatToPerson(value))))
      .groupByKey()
    logger.println("group成功" + nameGroupData.first())
    logger.flush()
    nameGroupData.foreach(value => trainGroup(value._2))


  }

  def formatToPerson(inputPerson :(String, String, String, String, String, String, String, String)):
 Person ={
//    class Person(val partner: Array[String], val org: String, val subject: Array[String]
//                 , val journal: String, val place: Array[String])
    val (kid,name,creatorall,subject,province,address,institute,journal) = inputPerson
    new Person(
      CheckSamePerson.getPartner(name,creatorall),
      institute,
      CheckSamePerson.getSubject(subject) union CheckSamePerson.getSubject(journal),
      journal,
      CheckSamePerson.getPlace(address) union CheckSamePerson.getPlace(institute) union CheckSamePerson.getPlace(province)
    )
  }
  def trainGroup(nameGroup: Iterable[(String,Person)])
  : Unit = {
    val persongArray = nameGroup.toArray
    val groupCount = persongArray.length

    for (i <- 0 to groupCount - 2) {
      for (j <- i + 1 to groupCount - 1) {
        val result:Double = if(persongArray(i)._1 == persongArray(j)._1) 1 else 0
       CheckSamePerson.train(result,persongArray(i)._2,persongArray(j)._2)
      }
    }
  }
}
