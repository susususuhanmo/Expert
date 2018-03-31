package com.zstu.libdata.StreamSplit.test

import com.zstu.libdata.StreamSplit.function.{CommonTools, ReadData, WriteData}

/**
  * Created by Administrator on 2018/3/14 0014.
  */
object sci {
  case class simple(guid:String,AU:String,C1:String,
                    organization:String, organ:String,
                    authorOrgan:String,authorOrgan1:String)

  def makeOrganization(C1:String) ={
    if(isNullOrBlank(C1)) null else {
      val reg = """(\s*\[.*?\]\s*)""".r
      reg.replaceAllIn(C1,"")
    }
  }
  def makeOrgan(organization:String) ={
    if(isNullOrBlank(organization)) null else {
      val organArray = organization.split(";")
      val firstLevelOrganArray = organArray.map(str =>
        if(str.indexOf(',') >=0) str.substring(0,str.indexOf(',')).trim
        else str)
      firstLevelOrganArray.reduce(_+";"+_)
    }
  }
  def toShortSpell(name:String) ={
    if(isNullOrBlank(name)) null
    else if(name.indexOf(',')<0)  name
    else {
      val firstName = name.substring(0,name.indexOf(','))
      val lastName = name.substring(name.indexOf(", ")+2)
       var shortLastName =   lastName.charAt(0).toString
      if(lastName.indexOf(" ")>0&&lastName.indexOf(" ")+2<lastName.length -1) shortLastName = shortLastName+lastName.substring(lastName.indexOf(' ')+1,lastName.indexOf(' ')+2)
      if(lastName.indexOf("-")>0&&lastName.indexOf("-")+2<lastName.length -1) shortLastName = shortLastName+lastName.substring(lastName.indexOf('-')+1,lastName.indexOf('-')+2)
   firstName+", " +shortLastName
    }
  }
  def getName(C1Str: String) = {
    val reg = """(?<=\[)(.*?)(?=\])""".r
    val names:String =  try{
      reg findFirstIn C1Str get
    } catch{

      case _ => null
    }
    names
  }
  def getShortSpellName(C1Str: String):Array[String] ={
    if(isNullOrBlank(C1Str)) null else {

      val reg = """(?<=\[)(.*?)(?=\])""".r
      val names:String =  try{
        reg findFirstIn C1Str get
      } catch{

        case _ => null
      }
      if(isNullOrBlank(names)) {
//        println("getShortSpellName"+ C1Str)
        return null
      }
      val nameArray = names.split(";").map(s => s.trim)
      val shortSpellArray = nameArray.map(name => toShortSpell(name))


      shortSpellArray
    }
  }
  def findOrganization(name:String,orgArray:Array[String])={
var rtn = ""
orgArray.foreach(C1Str=>{
  val shortNameArray = getShortSpellName(C1Str)
  val org = makeOrganization(C1Str)
if(shortNameArray==null) rtn=""
else if(shortNameArray.contains(name)) {
   rtn = org.trim
 }

})

rtn
  }
  def findOrgan(name:String,orgArray:Array[String])={
    var rtn = ""
    orgArray.foreach(C1Str=>{
      val shortNameArray = getShortSpellName(C1Str)
      val org = makeOrgan(makeOrganization(C1Str))
      //println("----------------------")
      //  println("oriname" + name)
      //  println("shortSpelled")
      //  shortNameArray.foreach(println)
      //  println("----------------------")
      if(shortNameArray==null) rtn=""
      else if(shortNameArray.contains(name)) {
        rtn = org.trim
      }

    })

    rtn
  }
  def isNullOrBlank(str: String) = if(str==null) true else if(str.trim.length ==0) true else false
//  def makeAuthorOrgan(C1:String,AU:String): String ={
//    if(isNullOrBlank(C1)&& isNullOrBlank(AU)) null
//    else {
////      var org =if(!isNullOrBlank(organization)) organization else  ""
////      var name = if(!isNullOrBlank(AU)) organization else  ""
//    val nameArray = if(!isNullOrBlank(AU)) {
//
//  val nA = AU.split(";").map(s => s.trim).filter(_!=null)
//  if (nA!=null && !nA.isEmpty) nA else null
//} else null
//      if(nameArray==null) return null
//      val orgArray = if(!isNullOrBlank(C1)) {
//        if(C1.indexOf('[')>=0)
//          C1.replace("; [",";; [").split(";;").map(s =>s.trim).filter(_!=null)
//        else  C1.split(";").map(s =>s.trim).filter(_!=null)
//      } else null
//
//      var num = -1
//      nameArray.map(name => if (isNullOrBlank(C1)) "{" + "\"name\": \""+name+"\"" + "}"
//      else if(C1.indexOf('[')<0) {
//        num = num+1
//
//        if(orgArray.length-1>=num)
//          "{" + "\"name\": \""+name+"\""  + "," + "\"org\": \""+
//            orgArray(num) +
//            "\""  + "}"
//        else "{" + "\"name\": \""+name+"\"" + "}"
//      }
//      else {
//        "{" + "\"name\": \""+name+"\""  + "," + "\"org\": \""+findOrganization(name, orgArray)+"\""  + "}"
//      }).reduce(_+","+_)
//
//    }
//  }
def makeAuthorOrgan(C1:String,AU:String): String ={
  if(isNullOrBlank(C1)&& isNullOrBlank(AU)) null
  else {
    val orgArray = if(!isNullOrBlank(C1)) {
      if(C1.indexOf('[')>=0)
        C1.replace("; [",";; [").split(";;").map(s =>s.trim).filter(_!=null)
      else  C1.split(";").map(s =>s.trim).filter(_!=null)
    } else null
if(orgArray ==null) return null

    orgArray.map(C1Str => if(C1Str.indexOf('[')<0) {


      "{" + "\"org\": \""+C1Str+"\"" + "}"
    }
    else {
      val nameArray = getName(C1Str).split(";").map(_.trim)
      val org = makeOrganization(C1Str).trim
      nameArray.map(name =>"{" + "\"name\": \""+name+"\""  + "," + "\"org\": \""+org+"\""  + "}" )
        .reduce(_+";;"+_)

    }).reduce(_+";;"+_).split(";;").distinct.reduce(_+","+_)

  }
}
  def makeAuthorOrgan1(C1:String,AU:String): String ={
    if(isNullOrBlank(C1)&& isNullOrBlank(AU)) null
    else {
      //      var org =if(!isNullOrBlank(organization)) organization else  ""
      //      var name = if(!isNullOrBlank(AU)) organization else  ""

      val orgArray = if(!isNullOrBlank(C1)) {
        if(C1.indexOf('[')>=0)
          C1.replace("; [",";; [").split(";;").map(s =>s.trim).filter(_!=null)
        else  C1.split(";").map(s =>s.trim).filter(_!=null)
      } else null

      if( orgArray ==null) return null
      orgArray.map(C1Str => if(C1Str.indexOf('[')<0) {


        "{" + "\"org\": \""+C1Str+"\"" + "}"
      }
      else {
        val nameArray = getName(C1Str).split(";").map(_.trim)
        val org = makeOrgan(makeOrganization(C1Str)).trim
        nameArray.map(name =>"{" + "\"name\": \""+name+"\""  + "," + "\"org\": \""+org+"\""  + "}" )
          .reduce(_+";;"+_)

      }).reduce(_+";;"+_).split(";;").distinct.reduce(_+","+_)

    }
  }
//  def makeAuthorOrgan1(C1:String,AU:String): String ={
//    if(isNullOrBlank(C1)&& isNullOrBlank(AU)) null
//    else {
//      //      var org =if(!isNullOrBlank(organization)) organization else  ""
//      //      var name = if(!isNullOrBlank(AU)) organization else  ""
//      val nameArray = if(!isNullOrBlank(AU)) {
//
//        val nA = AU.split(";").map(s => s.trim).filter(_!=null)
//        if (nA!=null && !nA.isEmpty) nA else null
//      } else null
//      if(nameArray==null) return null
//      val orgArray = if(!isNullOrBlank(C1)) {
//        if(C1.indexOf('[')>=0)
//        C1.replace("; [",";; [").split(";;").map(s =>s.trim).filter(_!=null)
//        else  C1.split(";").map(s =>s.trim).filter(_!=null)
//      } else null
//
//     var num = -1
//      nameArray.map(name => if (isNullOrBlank(C1)) "{" + "\"name\": \""+name+"\"" + "}"
//      else if(C1.indexOf('[')<0) {
//        num = num+1
//         if(orgArray.length-1>=num)
//        "{" + "\"name\": \""+name+"\""  + "," + "\"org\": \""+
//          makeOrgan(orgArray(num)) +
//          "\""  + "}"
//        else "{" + "\"name\": \""+name+"\"" + "}"
//      }
//      else {
//        "{" + "\"name\": \""+name+"\""  + "," + "\"org\": \""+findOrgan(name, orgArray)+"\""  + "}"
//      }).reduce(_+","+_)
//
//    }
//  }

  def dealData(guid:String,AU:String,C1:String): simple ={
  simple(guid,AU,C1,makeOrganization(C1),makeOrgan(makeOrganization(C1))
    ,makeAuthorOrgan(C1,AU),makeAuthorOrgan1(C1,AU))

  }

//  {"name": "Baral, C", "org": "Arizona State Univ, Comp Sci, Tempe, AZ 85287 USA"},
//  {"name": "Chang, SF", "org": "Columbia Univ, Sch Engn & Appl Sci, New York, NY 10027 USA"}
  def main(args: Array[String]): Unit = {
// val AU = "Baral, C; Chang, SF; Curless, B; Dasgupta, P; Hirschberg, J; Jones, A"
//  val C1 ="[Baral, Chitta; Dasgupta, Partha] Arizona State Univ, Comp Sci, Tempe, AZ 85287 USA; [Chang, Shih-Fu] Columbia Univ, Sch Engn & Appl Sci, New York, NY 10027 USA; [Chang, Shih-Fu] Columbia Univ, Dept Elect Engn, New York, NY 10027 USA; [Chang, Shih-Fu; Hirschberg, Julia] Columbia Univ, Dept Comp Sci, New York, NY 10027 USA; [Curless, Brian] Univ Washington, Paul G Allen Sch Comp Sci & Engn, Seattle, WA 98195 USA; [Hirschberg, Julia] Columbia Univ, Comp Sci, New York, NY 10027 USA; [Jones, Anita] Univ Virginia, Charlottesville, VA 22903 USA"
//val result = dealData("123",AU,C1)
//println(result.organ)
//  println(result.organization)
//  println(result.authorOrgan)
//  println(result.authorOrgan1)
    val hiveContext=CommonTools.initSpark("sci")
//  val inputData = ReadData.readData165("T_WOS_SCI",hiveContext).select("guid","C1","AU")
  val inputData2 = ReadData.readData165("T_WOS_AHCI",hiveContext).select("guid","C1","AU")
  val inputData3 = ReadData.readData165("T_WOS_SSCI",hiveContext).select("guid","C1","AU")
  val inputData = inputData2.unionAll(inputData3)
  val inputRdd = inputData.map(r =>(r.getString(r.fieldIndex("guid")),r.getString(r.fieldIndex("C1")),r.getString(r.fieldIndex("AU"))))
val result = hiveContext.createDataFrame(inputRdd.map(r => dealData(r._1,r._3,r._2)))
WriteData.writeDataWangzhihong("T_WOS_Simple",result)


}


}
