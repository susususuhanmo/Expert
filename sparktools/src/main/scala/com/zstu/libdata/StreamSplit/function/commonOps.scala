package com.zstu.libdata.StreamSplit.function

import java.sql.{PreparedStatement, Types}

import com.zstu.libdata.StreamSplit.KafkaDataClean.ParseCleanUtil
import com.zstu.libdata.StreamSplit.kafka.cnkiClean
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext

import scala.util.parsing.json.JSON

/**
  * Created by Yuli_Yan on 2017/4/17.
  */
object commonOps {


  //todo 查重算法改进
  //找到匹配的记录
  def getDisMatchRecord(value: ((String, String, String, String, String, String, String, String)
    , Option[(String, String, String, String, String, String, String, String)])): Boolean = {
    //   (key, (title, journal, creator, id,institute,year,issue,volume))

    if (value._2.isEmpty)
      return false

    val data = value._2.get
    if (value._1._4 == data._4)
      return true
    if (getSimilarty(value._1._1, data._1, value._1._2, data._2, value._1._3, data._3) > 0.9) {
      //虽然key匹配度>0.9 ,但是只要year不同认为不匹配
      if (value._1._6 != "" && data._6 != "" && value._1._6 != data._6)
        false
      //若issue 不同 则不匹配
      else if (value._1._7 != "" && data._7 != "" && data._7 != null && value._1._7 != null && value._1._7 != data._7)
        false
      else if (value._1._8 != "" && data._8 != "" && data._8 != null && value._1._8 != null && value._1._8 != data._8)
        false
      else
        true
    } else {
      false
    }
  }

  //在找到的5条匹配的记录中，返回相似度最高的记录
  def getHightestRecord(f: (String, Iterable[((String, String, String, String, String, String, String, String), Option[(String, String, String, String, String, String, String, String)])])): (String, String) = {
    val value = f._2
    if (value.size >= 2) {
      val firstData = value.take(1).toList.apply(0)
      var guid = firstData._2.get._4
      var simalirt = 0.0
      var high = getSimilarty(firstData._1._1, firstData._2.get._1, firstData._1._2, firstData._2.get._2, firstData._1._3, firstData._2.get._3)
      for (i <- 2 to value.size) {
        val second = value.take(i).toList.apply(0)
        simalirt = getSimilarty(second._1._1, second._2.get._1, second._1._2, second._2.get._2, second._1._3, second._2.get._3)
        if (high < simalirt) {
          high = simalirt
          guid = second._2.get._4
        }
      }
      (f._1, guid)
    } else {
      (f._1, f._2.take(1).toList.apply(0)._2.get._4)
    }
  }

  val sqlUrl = "jdbc:sqlserver://192.168.1.160:1433;DatabaseName=Log;"
  val userName = "fzj"
  val passWord = "fzj@zju"


  /**
    * 将数据insert进数据库
    *
    * @param tableName
    * @param hiveContext
    * @param writeErrorRDD
    * @return
    */
  def insertData(tableName: String, hiveContext: HiveContext, writeErrorRDD: RDD[(String, String)]) = {
    val option = Map("url" -> sqlUrl, "user" -> userName, "password" -> passWord, "dbtable" -> tableName,
      "driver" -> "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    val data = hiveContext.read.format("jdbc").options(option).load()
    data.registerTempTable("error")
    val errorData = hiveContext.createDataFrame(writeErrorRDD)
    errorData.registerTempTable("insertData")
    hiveContext.sql("insert into error select * from insertData")
  }

  def getJournalKay(title: String, journal: String): String = {
    val subTitle = cutStrOps(title, 6)
    val subJournal = cutStrOps(journal, 4)
    val key = subTitle + subJournal
    key
  }

  /**
    * hive中的数据进行转换
    *
    * @param r
    * @return
    */
  def transformRdd(r: Row) = {

    val title = cnkiOps.cleanTitleForMatch(r.getString(r.fieldIndex("title")))
    val journal = cnkiOps.cleanJournal(r.getString(r.fieldIndex("journal")))
    val year = r.getString(r.fieldIndex("year"))
    val institute = cnkiOps.getFirstInstitute(cnkiOps.cleanInstitute(r.getString(r.fieldIndex("instituteAll"))))
    var subTitle = cutStrOps(title, 6)
    var subJournal = cutStrOps(journal, 4)
    var creator = cnkiOps.getFirstCreator(cnkiOps.cleanAuthor(r.getString(r.fieldIndex("creatorAll"))))
    val id = r.getString(r.fieldIndex("id"))
    val issue = deleteZero.deleteZero(r.getString(r.fieldIndex("issue")))
    val volume = deleteZero.deleteZero(r.getString(r.fieldIndex("volume")))
    if (subTitle == null) subTitle = ""
    if (subJournal == null) subJournal = ""
    if (creator == null) creator = ""
    val key = subTitle + subJournal
    (key, (title, journal, creator, id, institute, year, issue, volume))
  }

  def transformRdd_cnki_simplify(r:Row) ={
    val id = r.getString(r.fieldIndex("GUID"))
    var creator =r.getString(r.fieldIndex("creator"))
    var title=r.getString(r.fieldIndex("title"))
    var journal =r.getString(r.fieldIndex("journal"))
    var journal2 =r.getString(r.fieldIndex("journal2"))
    journal = getData.getChineseJournal(journal,journal2)
    journal =cnkiOps.cleanJournal(journal)
    val year=r.getString(r.fieldIndex("year"))
    var institute =r.getString(r.fieldIndex("institute"))
    var issue = deleteZero.deleteZero(r.getString(r.fieldIndex("issue")))
    var volume = null
    if( title!="" && title !=null ) title = cnkiOps.cleanTitleForMatch(title.toUpperCase)
    if( journal !="" && journal !=null) journal = cnkiOps.cleanJournal(journal)
    if(institute !="" && institute !=null){institute = cnkiOps.getFirstInstitute(cnkiOps.cleanInstitute(institute))}
    if( creator!="" && creator !=null) creator = cnkiOps.getFirstCreator(cnkiOps.cleanAuthor(creator))
    val key = cutStrOps(title, 6) + cutStrOps(journal, 4)

    (key, (title, journal, creator, id,institute,year,issue,volume))
  }






  def transformRdd_wf_simplify(r:Row) ={
    val id = r.getString(r.fieldIndex("GUID"))
    var creator =r.getString(r.fieldIndex("creator"))
    var title=r.getString(r.fieldIndex("title"))
    var journal =r.getString(r.fieldIndex("journal_name"))
    var journal2 =r.getString(r.fieldIndex("journal_alt"))
    val year=r.getString(r.fieldIndex("year"))
    var institute =r.getString(r.fieldIndex("institute"))
    var issue = r.getString(r.fieldIndex("issue"))
    var volume = deleteZero.deleteZero(getData.getVolumeWF(issue))
    issue = deleteZero.deleteZero(getData.getIssueWF(issue))

    if( creator!="" && creator !=null) creator = cnkiOps.getFirstCreator(cnkiOps.cleanAuthor(creator))


    if( title!="" && title !=null ) title = cnkiOps.cleanTitleForMatch(title.toUpperCase)
    if( journal !="" && journal !=null) journal = cnkiOps.cleanJournal(journal)
    if( journal2 !="" && journal2 !=null) journal2 = cnkiOps.cleanJournal(journal2)
    if (journal == null || journal.equals(""))     journal = journal2
    if(institute !="" && institute !=null)      institute = cnkiOps.getFirstInstitute(cnkiOps.cleanInstitute(institute))
    val key = cutStrOps(title, 6) + cutStrOps(journal, 4)
    (key, (title, journal, creator, id,institute,year,issue,volume))
  }










  def transformRdd_vip_simplify(r:Row) ={
    val id = r.getString(r.fieldIndex("GUID"))
    var creator =r.getString(r.fieldIndex("creator_2"))
    var title=r.getString(r.fieldIndex("title"))
    var journal =r.getString(r.fieldIndex("journal_name"))
    var journal2 =r.getString(r.fieldIndex("journal_2"))
    val year=r.getString(r.fieldIndex("year"))
    var institute =r.getString(r.fieldIndex("institute_2"))
    var issue = r.getString(r.fieldIndex("issue"))

    var volume = deleteZero.deleteZero(getData.getVolumeVIP(issue))
    issue = deleteZero.deleteZero(getData.getIssueVIP(issue))


    if( creator!="" && creator !=null) creator = cnkiOps.getFirstCreator(cnkiOps.cleanAuthor(creator))


    if( title!="" && title !=null ) title = cnkiOps.cleanTitleForMatch(title.toUpperCase)


    if( journal2 !="" && journal2 !=null) journal2 = cnkiOps.cleanJournal(journal2)
    if( journal !="" && journal !=null) journal = cnkiOps.cleanJournal(journal)

    journal = getData.chooseNotNull(journal,journal2)
    journal = cnkiOps.cleanJournal(journal)
    if(institute !="" && institute !=null)      institute = cnkiOps.getFirstInstitute(cnkiOps.cleanInstitute(institute))
    val key = cutStrOps(title, 6) + cutStrOps(journal, 4)
    (key, (title, journal, creator, id,institute,year,issue,volume))
  }

















  /**
    * 截取指定长度的字符串
    *
    * @param str
    * @param length
    * @return
    */
  def cutStrOps(str: String, length: Int): String = {
    if (str == null) return null
    if (str.equals("null")) return null
    if (str.length >= length) {
      return str.substring(0, length)
    } else str
  }


  def getSimilarty(inputTitle: String, dbTitle: String, inputJournal: String
                   , dbJournal: String, inputCreator: String, dbCreator: String): Double = {
    //title0.7 journal 0.2 creator0.1
    //    若creator没有 title0.8 journal 0.2
    val titleScore = LevenshteinDistance.score(inputTitle, dbTitle)
    val journalScore = LevenshteinDistance.score(inputJournal, dbJournal)
    if (inputCreator == null || inputCreator == "" || dbCreator == null || dbCreator == "")
      titleScore * 0.8 + journalScore * 0.2
    else
      titleScore * 0.7 + journalScore * 0.2 + LevenshteinDistance.score(inputCreator, dbCreator) * 0.1
  }


  /**
    * 将错误的记录过滤出来
    *
    * @return
    */
  def filterErrorRecord(value: (String, String, String, String, String, String, String, String)): Boolean = {
    //    (key, (title, journal, creator, id,institute,year))


    if (value == None) return true
    if (value._1 == null || value._1.equals("")) return true
    if (value._2 == null || value._2.equals("")) return true
    if (value._6 == null || value._6.equals("")) return true
    else
      return false
  }


  def main(args: Array[String]): Unit = {

    //    val title = cnkiOps.cleanTitleForMatch("DETECTION OF HEAVY METALS IN AQUEOUS SOLUTION USING PGNAA TECHNIQUE")
    //    val journal = cnkiOps.cleanJournal("核技术：英文版")
    //
    //    var subTitle = cutStrOps(title, 6)
    //    var subJournal = cutStrOps(journal, 4)
    //
    //    if (subTitle == null) subTitle = ""
    //    if (subJournal == null) subJournal = ""
    //
    //    val key = subTitle + subJournal
    //    println(key)



    //    DETECT核技术英

    var journal = "核技术：英文版"
    var journal2 = "核技术：英文版[SCIE][CAS][CSCD]"
    var title = "Detection of heavy metals in aqueous solution using PGNAA technique"
    if( title!="" && title !=null ) title = cnkiOps.cleanTitleForMatch(title.toUpperCase)


    if( journal2 !="" && journal2 !=null) journal2 = cnkiOps.cleanJournal(journal2)
    if( journal !="" && journal !=null) journal = cnkiOps.cleanJournal(journal)
    if(journal.equals("") || journal == null) journal = journal2
    //    journal = getData.chooseNotNull(journal,journal2)
    journal = cnkiOps.cleanJournal(journal)
    val key = cutStrOps(title, 6) + cutStrOps(journal, 4)
    println(key)
  }
}
