package com.zstu.libdata.StreamSplit.kafka

import java.io.PrintWriter
import java.sql.{PreparedStatement, Types}
import java.text.SimpleDateFormat
import java.util.Date

import com.zstu.libdata.StreamSplit.KafkaDataClean.ParseCleanUtil
import com.zstu.libdata.StreamSplit.function.{cnkiOps, deleteZero, getData}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row}

import scala.util.parsing.json.JSON

/**
  * Created by Yuli_Yan on 2017/4/17.
  */
object commonClean {

  val logger1 = new PrintWriter("./DataCleanCommon.log")

  logUtil("------------开始运行 ---------------")

  def logUtil(info: String): Unit = {
    if (logger1 != null) {
      logger1.println(info + "\r\n")
      logger1.flush()
    }
  }

  def getNowDate():String={
    var now:Date = new Date()
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var hehe = dateFormat.format( now )
    hehe
  }




  val orgdata_sqlUrl = "jdbc:sqlserver://192.168.1.165:1433;DatabaseName=WangZhihong;"
  val orgdata_userName = "ggm"
  val orgdata_passWord = "ggm@092011"


  val sqlUrl = "jdbc:sqlserver://192.168.1.50:1433;DatabaseName=DiscoveryV2;"
  val userName = "ggm"
  val passWord = "ggm@zju"


  /**
    * 通过hive 从数据库中读取数据
    *
    * @param tableName
    * @param hiveContext
    * @return
    */
  def readData(tableName: String, hiveContext: HiveContext): DataFrame = {
    val option = Map("url" -> sqlUrl, "user" -> userName, "password" -> passWord, "dbtable" -> tableName,
      "driver" -> "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    val data = hiveContext.read.format("jdbc").options(option).load()
    data
  }

  def readDataOrg(tableName: String, hiveContext: HiveContext): DataFrame = {
    val option = Map("url" -> orgdata_sqlUrl, "user" -> orgdata_userName, "password" -> orgdata_passWord, "dbtable" -> tableName,
      "driver" -> "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    val data = hiveContext.read.format("jdbc").options(option).load()
    data
  }

  /**
    * 将数据insert进数据库
    *
    * @param tableName
    * @param hiveContext
    * @param writeErrorRDD
    * @return
    */
  def insertData(tableName: String, hiveContext: HiveContext, writeErrorRDD: RDD[(String, String)]) = {
    val option = Map("url" -> sqlUrl, "user" -> "ggm", "password" -> "ggm@zju", "dbtable" -> tableName,
      "driver" -> "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    val data = hiveContext.read.format("jdbc").options(option).load()
    data.registerTempTable("error")
    val errorData = hiveContext.createDataFrame(writeErrorRDD)
    errorData.registerTempTable("insertData")
    hiveContext.sql("insert into error select * from insertData")
  }

  def insertAllData2Log(tableName: String, hiveContext: HiveContext, writeErrorRDD: RDD[(String, Int,String,String)]) = {
    val  option = Map("url" -> orgdata_sqlUrl, "user" -> orgdata_userName, "password" -> orgdata_passWord, "dbtable" -> tableName,
      "driver" -> "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    val data = hiveContext.read.format("jdbc").options(option).load()
    data.registerTempTable("alldatatable")
    val errorData = hiveContext.createDataFrame(writeErrorRDD)
    errorData.registerTempTable("insertData")
    hiveContext.sql("insert into alldatatable select * from insertData")
  }

  def insertDataOrgErrorLog(tableName: String, hiveContext: HiveContext, writeErrorRDD: RDD[(String,String, Int,String)]) = {
    val  option = Map("url" -> orgdata_sqlUrl, "user" -> orgdata_userName, "password" -> orgdata_passWord, "dbtable" -> tableName,
      "driver" -> "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    val data = hiveContext.read.format("jdbc").options(option).load()
    data.registerTempTable("error")
    val errorData = hiveContext.createDataFrame(writeErrorRDD)
    errorData.registerTempTable("insertData")
    hiveContext.sql("insert into error select * from insertData")
  }

  def getJournalKay(title:String,journal:String): String ={
    val subTitle = cutStr(title, 6)
    val subJournal = cutStr(journal, 4)
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
    // value 里面的数据不应该进行截取
    val title = r.getString(r.fieldIndex("title")).toUpperCase
    val journal = r.getString(r.fieldIndex("journal"))
    val year = r.getString(r.fieldIndex("year"))
    val institute = r.getString(r.fieldIndex("instituteAll"))
    var subTitle = cutStr(title, 6)
    var subJournal = cutStr(journal, 4)
    var creator = r.getString(r.fieldIndex("creatorAll"))
    val id = r.getString(r.fieldIndex("id"))
    if (subTitle == null) subTitle = ""
    if (subJournal == null) subJournal = ""

    var creator_tmp=""
    var creator_tmp2=""
    val creaotr_org = creator
    try{
      creator_tmp= cnkiClean.cleanAuthor(creator)
      if( creator!="" && creator !=null) creator_tmp2 = cnkiClean.getFirstCreator(creator_tmp)
      creator = creator_tmp2
    }catch{
      case ex: Exception => logUtil("---transformRdd, clean author error，错误原因为---" + ex.getMessage)
        logUtil("------" +id+"------creator_org:"+creaotr_org+"------creator_tmp:"+creator_tmp+"-------creator_tmp2:"+ creator_tmp2+"------")
    }

    val key = subTitle + subJournal
    (key, (title, journal, creator, id,institute,year))
  }





  /**
    * 作者表数据匹配
    *
    * @param f
    */
  def getMatchMap(f: Row) = {
    val id = f.getLong(f.fieldIndex("id")).toString
    val name = f.getString(f.fieldIndex("name")).toString
    val partOrgan = f.getString(f.fieldIndex("partOrgan")).toString
    val key = name + partOrgan
    (key, id)
  }

  /**
    * 截取指定长度的字符串
    *
    * @param str
    * @param length
    * @return
    */
  def cutStr(str: String, length: Int): String = {
    if (str == null) return null
    if (str.equals("null")) return null
    if (str.length >= length) {
      return str.substring(0, length)
    } else str
  }

  def filterInnerSameGuid(f1:(String, String, String, String, String,String),f2:(String, String, String, String, String,String)):Boolean = {
    var flag = false
    if(f1._4 == f2._4)
      flag = true
    flag
  }





  def StringCompLessThan(A:String,B:String):Boolean = {
    var flag = true
    if(A.compareToIgnoreCase(B) < 0)
      flag = true
    else
      flag = false
    flag
  }








  /**
    * 判断是否为核心期刊
    *
    * @param row
    * @return
    */
  def matchCoreJournal(row: (String, ((String, String, String), Option[String]))): Boolean = {
    if (row._2._2 == None) {
      //没有匹配到说明不是核心期刊,直接进行insert操作
      return false
    } else {
      //不为空，说明有匹配到的数据，是核心期刊
      true
    }
  }

  def sourceCoreRecord(row: (String, ((String, String, String), Option[String]))): Boolean = {
    if (row._2._2 == None) {
      //没有匹配到说明不是核心期刊,直接进行insert操作
      return true
    } else {
      //不为空，说明有匹配到的数据，是核心期刊
      false
    }
  }







  /**
    * 将原数据解析
    *
    * @param jsonMap
    */
  def parseVipSourceJson(jsonMap: Map[String, Any]) = {
    var creator = ""
    var creatorAll = ""
    var title = ""
    var journal = ""
    var id = ""
    var institute = ""
    var abstractcont = ""
    var abstractcont_alt  = ""
    var titleAlt = ""
    var creatorAlt = ""
    var keyWord = ""
    var keyWordAlt = ""
    var instituteAll = ""
    var year = ""
    var issue = ""
    var url = ""
    if (isUseful(jsonMap, "guid")) id = jsonMap("guid").toString
    if (isUseful(jsonMap, "title")) title = cnkiClean.cleanTitle(jsonMap("title").toString)
    if (isUseful(jsonMap, "titleAlt")) titleAlt = jsonMap("titleAlt").toString
    if (isUseful(jsonMap, "creator")) creator = cnkiClean.getFirstCreator(cnkiClean.cleanAuthor(jsonMap("creator").toString))
    if (isUseful(jsonMap, "creator_all")) creatorAlt = cnkiClean.cleanAuthor(jsonMap("creator_all").toString)
    if (isUseful(jsonMap, "creator")) creatorAll = cnkiClean.cleanAuthor(jsonMap("creator").toString)
    if(creatorAll == null || creatorAll.equals("")){
      creatorAll = creator
    }
    if (isUseful(jsonMap, "keyword")) keyWord = cnkiClean.cleanKeyWord(jsonMap("keyword").toString)
    if (isUseful(jsonMap, "keyword_alt")) keyWordAlt = cnkiClean.cleanKeyWord(jsonMap("keyword_alt").toString)
    if (isUseful(jsonMap, "institute_2")) instituteAll = cnkiClean.cleanInstitute(jsonMap("institute_2").toString)
    if (isUseful(jsonMap, "year")) year = jsonMap("year").toString
    if (isUseful(jsonMap, "journal_2")) journal = cnkiClean.cleanUnJournal(jsonMap("journal_2").toString)
    if (isUseful(jsonMap, "institute_2")) institute = cnkiClean.getFirstInstitute(cnkiClean.cleanInstitute(jsonMap("institute_2").toString))
    if (isUseful(jsonMap, "issue")) issue = jsonMap("issue").toString
    if (isUseful(jsonMap, "url")) url = jsonMap("url").toString
    if (isUseful(jsonMap, "abstract")) abstractcont = jsonMap("abstract").toString
    if (isUseful(jsonMap, "abstract_alt")) abstractcont_alt = jsonMap("abstract_alt").toString
    (id, (id, title, titleAlt, creator, creatorAlt, creatorAll, keyWord, keyWordAlt, institute, instituteAll, year, journal, issue, url,abstractcont,abstractcont_alt))
  }




  /**
    * 清理 新作者数据
    *
    * @param f
    */
  def dealDate(f: (String, (String, Option[String]))): Boolean = {
    if (f._2._2 == None)
      true
    else false
  }

  /**
    * 清理 旧作者数据
    *
    * @param f
    */
  def dealOldData(f: (String, (String, Option[String]))): Boolean = {
    if (f._2._2 == None)
      false
    else true
  }

  /**
    * 判断集合中是否包含key字段字段，同时判断该字段是否为null
    *
    * @param jsonMap
    * @param key
    * @return
    */
  def isUseful(jsonMap: Map[String, Any], key: String): Boolean = {
    if (jsonMap.contains(key) && jsonMap(key) != null)
      true
    else false
  }

  /**
    * 将错误的记录过滤出来
    *
    * @return  true是错误数据  false 是正确数据
    */
  def filterErrorRecord(value: (String, String, String, String, String,String)): Boolean = {
    if (value == None) {
      //logUtil("------------filterErrorRecord1 --"+value+"-------------")
      return true
    }
    if (value._1 == null || value._1.equals("")) {
      //logUtil("------------filterErrorRecord2 --"+value+"-------------")
      return true
    }
    if (value._2 == null || value._2.equals("")){
      //logUtil("------------filterErrorRecord3 --"+value+"-------------")
      return true
    }
    false
  }

  /**
    * 获取格式正确的记录
    *
    * @param _2
    * @return
    */
  def filterTargetRecord(_2: (String, String, String, String, String,String)): Boolean = {
    if (_2 == None) {
      //logUtil("------------filterTargetRecord --"+value+"-------------")
      return false
    }
    if (_2._1 == null || _2._1.equals("")) return false
    if (_2._2 == null || _2._2.equals("")) return false
    true
  }
  def cleanIssue(issue: String,issuetype:Int): Array[String] = {
    var retarray:Array[String] = new Array[String](3)
    if (issuetype == 4) {
      //vip的issue
      val array: Array[String] = ParseCleanUtil.cleanVipIssue(issue)
      retarray= Array(array(2),array(1),array(3))
    }
    if (issuetype == 2) {
      //cnki的issue
      val array: Array[String] = ParseCleanUtil.cleanCnkiIssue(issue)
      retarray= Array(array(2),array(1),array(3))
    }
    if (issuetype == 8) {
      //wf的issue
      val array: Array[String] = ParseCleanUtil.cleanWfIssueNew(issue)
      retarray= Array( array(0),array(1),array(2))
    }
    retarray
  }
  /**
    * 设置数据库数据
    *
    * @param stmt
    * @param f
    */
  def setData4newdata(stmt: PreparedStatement, f: (String, String, String, String, String, String, String, String, String, String, String, String, String, String, Int, Int, String,String,String,String,String,(String,String,String)), source: Int) = {
    //DataCleanForAll.logUtil("---当前的类型为:---"+source)
    if (insertJudge(f._1))
      stmt.setString(1, f._1)
    else stmt.setNull(1, Types.VARCHAR)
    if (insertJudge(f._2))
      stmt.setString(2, f._2)
    else stmt.setNull(2, Types.NVARCHAR)
    if (insertJudge(f._3))
      stmt.setString(3, f._3)
    else stmt.setNull(3, Types.NVARCHAR)
    if (insertJudge(f._4))
      stmt.setString(4, f._4)
    else stmt.setNull(4, Types.NVARCHAR)
    if (insertJudge(f._5))
      stmt.setString(5, f._5)
    else stmt.setNull(5, Types.NVARCHAR)
    if (insertJudge(f._6))
      stmt.setString(6, f._6)
    else stmt.setNull(6, Types.NVARCHAR)
    if (insertJudge(f._7))
      stmt.setString(7, f._7)
    else stmt.setNull(7, Types.NVARCHAR)
    if (insertJudge(f._8))
      stmt.setString(8, f._8)
    else stmt.setNull(8, Types.NVARCHAR)
    if (insertJudge(f._9))
      stmt.setString(9, f._9)
    else stmt.setNull(9, Types.NVARCHAR)
    if (insertJudge(f._10))
      stmt.setString(10, f._10)
    else stmt.setNull(10, Types.NVARCHAR)
    if (insertJudge(f._11))
      stmt.setString(11, f._11)
    else stmt.setNull(11, Types.VARCHAR)
    if (insertJudge(f._12))
      stmt.setString(12, f._12)
    else stmt.setNull(12, Types.NVARCHAR)
    if (insertJudge(f._13)) {
      if (source == 4) {
        //vip的issue
        val array: Array[String] = ParseCleanUtil.cleanVipIssue(f._13)
        stmt.setString(13, array(2))
        stmt.setString(18, array(1))
        stmt.setString(19, array(3))
      }
      if (source == 2) {
        //cnki的issue
        /*val array: Array[String] = ParseCleanUtil.cleanCnkiIssue(f._13)
        stmt.setString(13, array(2))
        stmt.setString(18, array(1))
        stmt.setString(19, array(3))*/
        stmt.setString(13, f._13)
        stmt.setNull(18, Types.NVARCHAR)
        if(insertJudge(f._21)){
          stmt.setString(19,f._21)
        }else{
          stmt.setNull(19, Types.NVARCHAR)
        }
      }
      if (source == 8) {
        //wf的issue
        val array: Array[String] = ParseCleanUtil.cleanWfIssue(f._13)
        stmt.setString(13, array(array.length - 1))
        stmt.setString(18, array(1))
        stmt.setNull(19, Types.VARCHAR)
      }
    }
    else stmt.setNull(13, Types.VARCHAR)
    if (insertJudge(f._14))
      stmt.setString(14, f._14)
    else stmt.setNull(14, Types.NVARCHAR)
    stmt.setInt(15, f._15)
    stmt.setInt(16, f._16)
    if (insertJudge(f._17)) //otherId
      stmt.setString(17, f._17)
    else
      stmt.setNull(17, Types.VARCHAR)
    if (insertJudge(f._18))  //datasource
      stmt.setString(20, f._18)
    else stmt.setNull(20, Types.VARCHAR)
    if (insertJudge(f._19))  //abstract
      stmt.setString(21, f._19)
    else stmt.setNull(21, Types.VARCHAR)
    if (insertJudge(f._20))  //abstract_alt
      stmt.setString(22, f._20)
    else stmt.setNull(22, Types.VARCHAR)

    if (insertJudge(f._22._1))  //subject
      stmt.setString(23,f._22._1)
    else stmt.setNull(23, Types.VARCHAR)
    if (insertJudge(f._22._2))  //doi
      stmt.setString(24,f._22._2)
    else stmt.setNull(24, Types.VARCHAR)
    if (insertJudge(f._22._3))  //issn
      stmt.setString(25,f._22._3)
    else stmt.setNull(25, Types.VARCHAR)
  }


  def setData4newdata2Journal2016(stmt: PreparedStatement, f: (String, String, String, String, String, String, String, String, String, String, String, String, String, String, Int, Int, String,String,String,String), source: Int): Unit = {
    //DataCleanForAll.logUtil("---当前的类型为:---"+source)
    if (insertJudge(f._1))
      stmt.setString(1, f._1)
    else stmt.setNull(1, Types.VARCHAR)
    if (insertJudge(f._2))
      stmt.setString(2, f._2)
    else stmt.setNull(2, Types.NVARCHAR)
    if (insertJudge(f._3))
      stmt.setString(3, f._3)
    else stmt.setNull(3, Types.NVARCHAR)
    if (insertJudge(f._4))
      stmt.setString(4, f._4)
    else stmt.setNull(4, Types.NVARCHAR)
    if (insertJudge(f._5))
      stmt.setString(5, f._5)
    else stmt.setNull(5, Types.NVARCHAR)
    if (insertJudge(f._6))
      stmt.setString(6, f._6)
    else stmt.setNull(6, Types.NVARCHAR)
    if (insertJudge(f._7))
      stmt.setString(7, f._7)
    else stmt.setNull(7, Types.NVARCHAR)
    if (insertJudge(f._8))
      stmt.setString(8, f._8)
    else stmt.setNull(8, Types.NVARCHAR)
    if (insertJudge(f._9))
      stmt.setString(9, f._9)
    else stmt.setNull(9, Types.NVARCHAR)
    if (insertJudge(f._10))
      stmt.setString(10, f._10)
    else stmt.setNull(10, Types.NVARCHAR)
    if (insertJudge(f._11))
      stmt.setString(11, f._11)
    else stmt.setNull(11, Types.VARCHAR)
    if (insertJudge(f._12))
      stmt.setString(12, f._12)
    else stmt.setNull(12, Types.NVARCHAR)
    if (insertJudge(f._13)) {
      if (source == 4) {
        //vip的issue
        val array: Array[String] = ParseCleanUtil.cleanVipIssue(f._13)
        stmt.setString(13, array(2))
        stmt.setString(16, array(1))
        stmt.setString(17, array(3))
      }
      if (source == 2) {
        //cnki的issue
        val array: Array[String] = ParseCleanUtil.cleanCnkiIssue(f._13)
        stmt.setString(13, array(2))
        stmt.setString(16, array(1))
        stmt.setString(17, array(3))
      }
      if (source == 8) {
        //wf的issue
        val array: Array[String] = ParseCleanUtil.cleanWfIssue(f._13)
        stmt.setString(13, array(array.length - 1))
        stmt.setString(16, array(1))
        stmt.setNull(17, Types.VARCHAR)
      }
    }
    else stmt.setNull(13, Types.VARCHAR)
    if (insertJudge(f._14))
      stmt.setString(14, f._14)
    else stmt.setNull(14, Types.NVARCHAR)
    stmt.setInt(15, f._15)

    if (insertJudge(f._19))  //abstract
      stmt.setString(21, f._19)
    else stmt.setNull(21, Types.VARCHAR)
    if (insertJudge(f._20))  //abstract_alt
      stmt.setString(22, f._20)
    else stmt.setNull(22, Types.VARCHAR)
  }

  /**
    * 设置数据库数据
    *
    * @param stmt
    * @param f
    */
  def setData(stmt: PreparedStatement, f: (String, String, String, String, String, String, String, String, String, String, String, String, String, String, Int, Int, String,String,String,String,String,(String,String,String)), source: Int) = {
    //DataCleanForAll.logUtil("---当前的类型为:---"+source)
    if (insertJudge(f._1))
      stmt.setString(1, f._1)
    else stmt.setNull(1, Types.VARCHAR)
    if (insertJudge(f._2))
      stmt.setString(2, f._2)
    else stmt.setNull(2, Types.NVARCHAR)
    if (insertJudge(f._3))
      stmt.setString(3, f._3)
    else stmt.setNull(3, Types.NVARCHAR)
    if (insertJudge(f._4))
      stmt.setString(4, f._4)
    else stmt.setNull(4, Types.NVARCHAR)
    if (insertJudge(f._5))
      stmt.setString(5, f._5)
    else stmt.setNull(5, Types.NVARCHAR)
    if (insertJudge(f._6))
      stmt.setString(6, f._6)
    else stmt.setNull(6, Types.NVARCHAR)
    if (insertJudge(f._7))
      stmt.setString(7, f._7)
    else stmt.setNull(7, Types.NVARCHAR)
    if (insertJudge(f._8))
      stmt.setString(8, f._8)
    else stmt.setNull(8, Types.NVARCHAR)
    if (insertJudge(f._9))
      stmt.setString(9, f._9)
    else stmt.setNull(9, Types.NVARCHAR)
    if (insertJudge(f._10))
      stmt.setString(10, f._10)
    else stmt.setNull(10, Types.NVARCHAR)
    if (insertJudge(f._11))
      stmt.setString(11, f._11)
    else stmt.setNull(11, Types.VARCHAR)
    if (insertJudge(f._12))
      stmt.setString(12, f._12)
    else stmt.setNull(12, Types.NVARCHAR)
    if (insertJudge(f._13)) {
      if (source == 4) {
        //vip的issue
        val array: Array[String] = ParseCleanUtil.cleanVipIssue(f._13)
        stmt.setString(13, array(2))
        stmt.setString(18, array(1))
        stmt.setString(19, array(3))
      }
      if (source == 2) {
        //cnki的issue
        //cnki的issue
        /*val array: Array[String] = ParseCleanUtil.cleanCnkiIssue(f._13)
        stmt.setString(13, array(2))
        stmt.setString(18, array(1))
        stmt.setString(19, array(3))*/
        stmt.setString(13, f._13)
        stmt.setNull(18, Types.NVARCHAR)
        if(insertJudge(f._21)){
          stmt.setString(19,f._21)
        }else{
          stmt.setNull(19, Types.NVARCHAR)
        }
      }
      if (source == 8) {
        //wf的issue
        val array: Array[String] = ParseCleanUtil.cleanWfIssue(f._13)
        stmt.setString(13, array(array.length - 1))
        stmt.setString(18, array(1))
        stmt.setNull(19, Types.VARCHAR)
      }
    }
    else stmt.setNull(13, Types.VARCHAR)
    if (insertJudge(f._14))
      stmt.setString(14, f._14)
    else stmt.setNull(14, Types.NVARCHAR)
    stmt.setInt(15, f._15)
    stmt.setInt(16, f._16)
    if (insertJudge(f._17)) //otherId
      stmt.setString(17, f._17)
    else{
      stmt.setNull(17, Types.VARCHAR)
    }
    if (insertJudge(f._18))  //datasource
      stmt.setString(20, f._18)
    else stmt.setNull(20, Types.VARCHAR)
    if (insertJudge(f._19))  //abstract
    //stmt.setString(21, f._19)
      stmt.setString(21, "")
    else stmt.setNull(21, Types.VARCHAR)
    if (insertJudge(f._20))  //abstract_alt
    //stmt.setString(22, f._20)
      stmt.setString(22, "")
    else stmt.setNull(22, Types.VARCHAR)


    if (insertJudge(f._22._1))  //subject
      stmt.setString(23,f._22._1)
    else stmt.setNull(23, Types.VARCHAR)
    if (insertJudge(f._22._2))  //doi
      stmt.setString(24,f._22._2)
    else stmt.setNull(24, Types.VARCHAR)
    if (insertJudge(f._22._3))  //issn
      stmt.setString(25,f._22._3)
    else stmt.setNull(25, Types.VARCHAR)
  }

  def setmatchData(stmt: PreparedStatement, f: (String, String, String, String, String),types:Int) = {
    if (insertJudge(f._1))
      stmt.setString(1, f._1)
    else stmt.setNull(1, Types.VARCHAR)
    //title, journal, creator, id, institute
    if (insertJudge(f._2))
      stmt.setString(2, f._2)
    else stmt.setNull(2, Types.VARCHAR)
    if (insertJudge(f._3))
      stmt.setString(3, f._3)
    else stmt.setNull(3, Types.VARCHAR)
    if (insertJudge(f._4))
      stmt.setString(4, f._4)
    else stmt.setNull(4, Types.VARCHAR)
    if (insertJudge(f._5))
      stmt.setString(5, f._5)
    else stmt.setNull(5, Types.VARCHAR)
    stmt.setInt(6,types)
  }


  def insertJudge(col: String): Boolean = {
    if (col == null || col.equals(""))
      return false
    else true
  }






  def main(args: Array[String]): Unit = {

  }
}
