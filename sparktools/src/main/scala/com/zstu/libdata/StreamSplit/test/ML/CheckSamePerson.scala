package com.zstu.libdata.StreamSplit.test.ML

import com.zstu.libdata.StreamSplit.test.ML.SamePersonLearning.logger

/**
  * Created by Administrator on 2017/11/20 0020.
  */

class Person(val partner: Array[String], val org: String, val subject: Array[String], val journal: String, val place: Array[String]) {

}

object CheckSamePerson {
  var partnerWeight = 0.2
  var orgWeight = 0.2
  var subjectWeight = 0.2
  var journalWeight = 0.2
  var placeWeight = 0.2
  var threshold = 0.8
  val alpha = 0.1
  val places: Array[String] = CityAndSubject.citis
  val subjects : Array[String] = CityAndSubject.subjects

  /**
    * 根据预测概率判断输入的两个人是否为同一人
    * @param person1
    * @param person2
    * @return
    */
  def isSamePerson(person1: Person, person2: Person): Boolean =
    if (predictResult(person1, person2) >= threshold) true else false
def predictLeaner(person1: Person, person2: Person) = {
  partnerWeight * getSimilarity(person1.partner, person2.partner) +
    orgWeight * (if (person1.org == person2.org) 1 else 0) +
    subjectWeight * getSimilarity(person1.subject, person2.subject) +
    journalWeight * (if (person1.journal == person2.journal) 1 else 0) +
    placeWeight * getSimilarity(person1.place, person2.place)
}
  /**
    * 预测输入的两个人为同一人的概率
    * @param person1
    * @param person2
    * @return
    */
  def predictResult(person1: Person, person2: Person): Double = {

    sigmod(predictLeaner(person1,person2))
  }

  /**
    *
    * @param array1 待比较数组，要求过滤过null值并且distinct过。
    * @param array2
    * @return
    */
  def getSimilarity(array1: Array[String], array2: Array[String]): Double = {
    if (array1 == null || array2 == null || array1.length == 0 || array2.length == 0) 0
    else {
      val length = array1.length + array2.length
      var sameNum = 0
      array1.foreach(value => if (array2.indexOf(value) >= 0) sameNum += 1)
      (sameNum * 2.0) / length
    }
  }

  /**
    * 从作者和所有作者中处理出合作者数组，若无合作者则返回null
    *
    * @param author
    * @param allAuthors
    * @return
    */
  def getPartner(author: String, allAuthors: String): Array[String] = {
    if (author == null || allAuthors == null) return null
    val allAuthorsArray = allAuthors.split(";")
    if (allAuthorsArray.indexOf(author) >= 0)
      allAuthorsArray(allAuthorsArray.indexOf(author)) = null
    val result = allAuthorsArray.filter(_ != null).filter(_.length > 0).distinct
    if (result.length == 0) null else result
  }

  def sigmod(learnerResult: Double): Double = 1 / (1 + (math.exp(-learnerResult)))

  def getPlace(stringMayHavePlace: String):Array[String] =
    places.filter(stringMayHavePlace.indexOf(_) >0)
  def getSubject(stringMayHaveSubject: String):Array[String] =
    subjects.filter(stringMayHaveSubject.indexOf(_) >0)

  def train(correctResult: Double,person1: Person,person2: Person): Unit = {

    partnerWeight = partnerWeight - alpha * (predictLeaner(person1,person2) - correctResult) *
      (partnerWeight * getSimilarity(person1.partner, person2.partner))
    orgWeight = orgWeight - alpha * (predictLeaner(person1,person2) - correctResult)*
      (orgWeight * (if (person1.org == person2.org) 1 else 0))
    subjectWeight = subjectWeight - alpha * (predictLeaner(person1,person2) - correctResult)*
      (subjectWeight * getSimilarity(person1.subject, person2.subject))
    journalWeight = journalWeight - alpha * (predictLeaner(person1,person2) - correctResult)*
      ( journalWeight * (if (person1.journal == person2.journal) 1 else 0))
    placeWeight = placeWeight - alpha * (predictLeaner(person1,person2) - correctResult)*
      ( placeWeight * getSimilarity(person1.place, person2.place))
  }
  def main(args: Array[String]): Unit = {
    val array1: Array[String] = Array()
    val array2 = Array("1", "2")
    val authors = getPartner("a", ";")
    println(authors)
    //authors.foreach(println)
  }
}
