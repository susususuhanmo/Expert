package com.zstu.libdata.StreamSplit.test

import com.zstu.libdata.StreamSplit.function.{CommonTools, ReadData, WriteData}

import scala.util.matching.Regex

/**
  * Created by Administrator on 2018/2/22 0022.
  */
object AIOps {

  private def GetData(dataStr: String,col:String): String =
    {


        val reg = new Regex("(?<="+col+")(.*?)(?=\\})")
      try{
        FindWithReg(FindWithReg(dataStr,reg)+"}","""(?<=\{)(.*?)(?=\})""".r)
      }catch{
        case _ =>null
      }

    }
  private def GetAuthor(str:String): String = GetData(str,"author")
  private def GetTitle(str:String): String = GetData(str,"title")
  private def FindWithReg(url : String,reg:Regex )={

      try{
        reg findFirstIn url get
      }catch {
        case e =>{
//          println("异常为"+e.getMessage)
//          println("数据为"+ url)
//          println("正则为"+reg)
          null
        }
      }

  }
  case class colum(resource:String,author:String,title:String,journal:String,volume:String,number:String,pages:String,year:String,
                   url:String,doi:String,timestamp:String,biburl:String,bibsource:String)
  def DealDataStr(dataStr:String) ={

    val author = GetData(dataStr,"author")
    val title = GetData(dataStr,"title")
    val journal = GetData(dataStr,"journal")
    val volume = GetData(dataStr,"volume")
    val number = GetData(dataStr,"number")
    val pages = GetData(dataStr,"pages")
    val year = GetData(dataStr,"year")
    val url = GetData(dataStr,"url")
    val doi = GetData(dataStr,"doi       ")
    val timestamp = GetData(dataStr,"timestamp")
    val biburl = GetData(dataStr,"biburl")
    val bibsource = GetData(dataStr,"bibsource")
    colum(dataStr,author,title,journal,volume,number,pages,year,url,doi,timestamp,biburl,bibsource)
  }
  def main(args: Array[String]): Unit = {


    val hiveContext=CommonTools.initSpark("AiOps")
    val aiData = ReadData.readData50("Crawler","t_AI_resource",hiveContext)
      .map(r=> r.getString(r.fieldIndex("Info")))
    val resultData = hiveContext.createDataFrame(aiData.map(str =>DealDataStr(str.replace("\n",""))))
    WriteData.writeData50("Crawler","t_AI",resultData)



  }
}
