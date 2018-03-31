package com.zstu.libdata.StreamSplit.function

import com.zstu.libdata.StreamSplit.function.CommonTools.{hasNoChinese, splitStr}
/**
  * Created by xiangjh on 2017/4/2.
  */
object cnkiOps {

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
      str.substring(0, length)
    } else str
  }


  def getFirstCreator(author: String): String = {
    if(author == null)null
    else {
      val authorArray = author.split(";")
      if(authorArray.length == 0) null
      else authorArray(0)
    }

  }

  /**
    * 清洗作者函数
    *
    * @return
    */
  def cleanAuthor(author: String): String = {
    if(author == null) return null
    if(hasNoChinese(author)) return author.replace("|!",";").replace("@@","")

    val author_tmp = deleteInvisibleChar.deleteInvisibleChar(author.toString)
      .replace("@@","").replace("|!",";")
    if(author_tmp == "") return null
    val creators: Array[String] = author_tmp.split(";")


  if(creators.isEmpty) return null
    var creator = ""
    for (i <- creators.indices) {
      val eachCreator = creators(i).trim()
      //把名字后面的方挂号去掉    於宁军[1,2]
      if(eachCreator ==null){
        creator += ""
      }else{
        val tmp: Array[String] = eachCreator.toString.split("\\[")
        if(tmp.isEmpty) creator += ""
        else {
          var creator_temp = tmp(0)
          creator_temp = DeleteCharIfIsLastNum.DeleteCharIfIsLastNum(creator_temp)
          creator += creator_temp + ";"
        }

      }
    }
    //把末尾的“；”去掉
    creator = creator.substring(0, creator.lastIndexOf(";"))
    creator
  }

  /**
    * 清洗标题字段
    *
    * @param title
    * @return
    */
  def cleanTitle(title: String): String = {
    if(title == null) null
    else {
      var value = deleteInvisibleChar.deleteInvisibleChar(title)
      //标准化处理 统一处理成大写
      value = value.toUpperCase
      value
    }
  }

  def cleanTitleForMatch(title: String): String = {
    if(title == null) null
    else {
      var value = deleteInvisibleChar.deleteInvisibleChar(title)
      //标准化处理 统一处理成大写
      value = value.toUpperCase
      GetReplacedStr.GetReplacedStr(value)
    }
  }

  //  def  cleanInstitute(institute: String): String = {
  //    def getStrBefore(str: String): String = {
  //      if (str == null) null
  //      else {
  //        if (str.indexOf(",") >= 0) str.substring(0, str.indexOf(","))
  //        else str
  //      }
  //    }
  //    if(institute == null) null
  //    else {
  //      val rtn = institute.replace("，", ",").replace("|!", ";")
  //        val rtnArray = splitStr(rtn).map(getStrBefore(_).trim).filter(s => s != "" && s != null)
  //        if (rtnArray.isEmpty) null
  //        else rtnArray.reduce(_ + ";" + _)
  //    }
  //
  //  }

  def isCnChar(ch: Char): Boolean = if(ch < 19968 || ch >19968+20901)  false else  true
  def isEnCHar(ch: Char) : Boolean = if((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')) true else false
  def isNum(ch: Char) : Boolean = if(ch >= '0' && ch <= '9') true else false
  def removeChacter(str: String) : String ={
    if(str == null) return null
    var rtn = str;
    if(!isEnCHar(rtn(0)) && !isCnChar(rtn(0)) && !isNum(rtn(0)))
      rtn = rtn.substring(1)

    val tail = rtn.length - 1
    if(tail<0) return null
    if(!isEnCHar(rtn(tail)) && !isCnChar(rtn(tail)) && !isNum(rtn(tail)))
      rtn = rtn.substring(0,tail )
    rtn
  }
  def cleanInstitute(institute: String): String ={
    if(institute == null) return null
    val rtn = RemovePostCodeNum.removePostCode(institute.replace("，",",").replace("|!",";"))
    def getStrBefore(str: String):String={
      if(str == null) null
      else {
        if(str.indexOf(",") >=0) str.substring(0,str.indexOf(","))
        else str
      }
    }
    if(rtn == null) null

    else {
      val rtnArray = splitStr(rtn)
        .filter(s => s != "" && s != null)
        .map(s =>RemoveCity.removeCity(s))
        .filter(s => s != "" && s != null)
      if (rtnArray.isEmpty) null
      else removeChacter(rtnArray.reduce(_ + ";" + _))
    }


  }
  def getFirstInstitute(institute: String): String = {
    if(institute == null) null
    else {
      if(institute.split(";").length > 0) institute.split(";")(0)
      else null
    }
  }






  /**
    * 清洗关键字字段
    *
    * @return
    */
  def cleanKeyWord(keys: String): String = {
    val keyWords: Array[String] = deleteInvisibleChar.deleteInvisibleChar(keys).split("\\|!")
    var keyWord = ""
    for (i <- keyWords.indices) {
      val eachKeyWord = keyWords(i).trim()
      keyWord += eachKeyWord + ";"
    }
    keyWord = keyWord.substring(0, keyWord.lastIndexOf(";"))
    keyWord
  }

  /**
    * 获取中文期刊名
    *
    * @param journal
    * @return
    */
  def cleanJournal(journal: String): String = {
    var journals = journal
    if (journals == null) {
      return null
    }
    journals = deleteInvisibleChar.deleteInvisibleChar(journal)
    journals = GetReplacedStr.GetReplacedStr(journals)
    journals

  }


  /**
    * 获取中文期刊名
    *
    * @param journal
    * @return
    */
  def cleanUnJournal(journal: String): String = {
    val journals = deleteInvisibleChar.deleteInvisibleChar(journal)
    var journalstr = ""
    if(journals == null)
      journalstr =""
    else{
      val tmp :Array[String] = journals.toString.split("\\[")
      journalstr = tmp(0)

    }
    journalstr
  }


  def main(args: Array[String]): Unit = {
    //  2016年第0卷第1期 9-页,共1页
    var a : String = null
    println(a.split(';')(0))


    //    adat
  }



}