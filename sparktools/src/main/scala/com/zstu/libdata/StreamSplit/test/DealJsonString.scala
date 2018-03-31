package com.zstu.libdata.StreamSplit.test

import java.io.PrintWriter

import scala.util.matching.Regex
import scala.util.parsing.json.JSON

/**
  * Created by Administrator on 2017/10/30 0030.
  */
object DealJsonString {
  val logger = new PrintWriter("./DealJson.txt")
  //  dealMapResult(map, "id"),
  //  dealMapResult(map, "title"),
  //  dealMapResult(map, "venue"),
  //  getAuthorsString(jsonString),
  //  dealMapResult(map, "year"),
  //  dealMapResult(map, "keywords"),
  //  dealMapResult(map, "fos"),
  //  dealMapResult(map, "n_citation"),
  //  dealMapResult(map, "page_start"),
  //  dealMapResult(map, "page_end"),
  //  dealMapResult(map, "doc_type"),
  //  dealMapResult(map, "lang"),
  //  dealMapResult(map, "publisher"),
  //  dealMapResult(map, "volume"),
  //  dealMapResult(map, "issue"),
  //  dealMapResult(map, "issn"),
  //  dealMapResult(map, "isbn"),
  //  dealMapResult(map, "doi"),
  //  dealMapResult(map, "url"),
  //  dealMapResult(map, "pdf"),
  //  dealMapResult(map, "abstract"),
  //  dealMapResult(map, "references")
  case class Aminer(id: String, title: String, venue: String, authors: String, year: String,
                    keywords: String, fos: String, n_citation: String,
                    page_start: String, page_end: String, doc_type: String,
                    lang: String, publisher: String, volume: String, issue: String,
                    issn: String, isbn: String, doi: String, url: String, pdf: String,
                    Abstract: String, reference: String)

  def getAuthorsString(jsonString: String): String = {
    if (jsonString.indexOf("\"authors\":") < 0) return null
    var rtn = jsonString.substring(jsonString.indexOf("\"authors\":")+10)

    rtn = rtn.substring(rtn.indexOf('[') + 1, rtn.indexOf(']'))
    rtn
  }

  def authorOps(authorAny: Any): (String, String) = {
    authorAny match {
      case authorList: List[Any] => {
        if (!authorList.isEmpty) {
          authorList(0) match {
            case double: Double => {
//              println("double")
              (null, null)
            }
            case string: String => {
//              println("string")
              (null,null)
            }
            case map: Map[String, Any] => authorOpsList(authorList)
            case _ => (null, null)
          }

        }
        else (null, null)
      }
      case _ => (null, null)
    }
  }

  def authorOpsList(authorList1: List[Any]): (String, String) = {
    authorList1 match {
      case authorList: List[Map[String, Any]] => {
        if (authorList == null) return (null, null)
        val nameArray1 =
          authorList.map(authorMap => if (getMap(authorMap, "name") == null) null else {
            getMap(authorMap, "name").toString
          }).toArray.filter(_ != null)

        val nameArray = nameArray1

        val orgArray: Array[String] = authorList.map(authorMap => if (getMap(authorMap, "org") == null) null else getMap(authorMap, "org").toString).toArray.filter(_ != null)
        val name = if (nameArray.isEmpty) null else nameArray.reduce(_ + ";" + _)
        //    println(name)
        val org = if (orgArray.isEmpty) null else orgArray.reduce(_ + ";" + _)
        //    println(org)
        (name, org)
      }
      case _ => (null,null)
    }
  }

  def dealList(list: Any) = {
    list match {
      case listString: List[String] => listString.reduce(_ + ";" + _)
      case string: String => string.toString
      case double: Double => double.toInt.toString
      case _ => null
    }
  }

  def parseAuthors(authorJson: String): (String, String) = {
    try {
      if (authorJson == null) return (null, null)
      val jsonSome = JSON.parseFull("{\"authors\":[" + authorJson + "]}")
      val a = if (jsonSome.isEmpty) null else jsonSome.get
      val result = a match {
        case map: Map[String, Any] => {
          val authorList = getMap(map, "authors")
          authorOps(authorList)
        }
        case _ => (null, null)
      }
      result
    }catch {
      case e: Exception => {
        logger.println("authorJson")
        logger.flush()
        (null,null)
      }
    }
  }

  def getMap(map: Map[String, Any], keyName: String) = {
//    println(map)
    if (map == null) null
    else if (map.contains(keyName)) map(keyName)
    else null
  }

  def dealMapResult(map: Map[String, Any], keyName: String) = dealList(getMap(map: Map[String, Any], keyName: String))

  def dealJsonString(jsonString: String) = {
    val jsonSome = JSON.parseFull(jsonString)
    val a = jsonSome.get
    val result = a match {
      case map: Map[String, Any] => {
        Aminer(
          dealMapResult(map, "id"),
          dealMapResult(map, "title"),
          dealMapResult(map, "venue"),
          getAuthorsString(jsonString),
          dealMapResult(map, "year"),
          dealMapResult(map, "keywords"),
          dealMapResult(map, "fos"),
          dealMapResult(map, "n_citation"),
          dealMapResult(map, "page_start"),
          dealMapResult(map, "page_end"),
          dealMapResult(map, "doc_type"),
          dealMapResult(map, "lang"),
          dealMapResult(map, "publisher"),
          dealMapResult(map, "volume"),
          dealMapResult(map, "issue"),
          dealMapResult(map, "issn"),
          dealMapResult(map, "isbn"),
          dealMapResult(map, "doi"),
          dealMapResult(map, "url"),
          dealMapResult(map, "pdf"),
          dealMapResult(map, "abstract"),
          dealMapResult(map, "references")

        )
      }
    }
    result
  }


  def main(args: Array[String]): Unit = {
//    val jsonStr1 = "{\n  \"id\": \"53e9ab9eb7602d970354a97e\",\n  \"title\": \"Data mining: concepts and techniques\",\n  \"authors\": [\n    {\n      \"name\": \"jiawei han\",\n      \"org\": \"department of computer science university of illinois at urbana champaign\"\n    },\n    {\n      \"name\": \"micheline kamber\",\n      \"org\": \"department of computer science university of illinois at urbana champaign\"\n    },\n    {\n      \"name\": \"jian pei\",\n      \"org\": \"department of computer science university of illinois at urbana champaign\"\n    }\n  ],\n  \"year\": 2000,\n  \"keywords\": [\n    \"data mining\",\n    \"structured data\",\n    \"world wide web\",\n    \"social network\",\n    \"relational data\"\n  ],\n  \"fos\": [\n    \"relational database\",\n    \"data model\",\n    \"social network\"\n  ],\n  \"n_citation\": 29790,\n  \"references\": [\n    \"53e99ef4b7602d97027c2346\",\n    \"53e9aa23b7602d970338fb5e\",\n    \"53e99cf5b7602d97025aac75\"\n  ],\n  \"doc_type\": \"book\",\n  \"lang\": \"en\",\n  \"publisher\": \"Elsevier\",\n  \"isbn\": \"1-55860-489-8\",\n  \"doi\": \"10.4114/ia.v10i29.873\",\n  \"pdf\": \"//static.aminer.org/upload/pdf/1254/370/239/53e9ab9eb7602d970354a97e.pdf\",\n  \"url\": [\n    \"http://dx.doi.org/10.4114/ia.v10i29.873\",\n    \"http://polar.lsi.uned.es/revista/index.php/ia/article/view/479\"\n  ],\n  \"abstract\": \"Our ability to generate and collect data has been increasing rapidly. Not only are all of our business, scientific, and government transactions now computerized, but the widespread use of digital cameras, publication tools, and bar codes also generate data. On the collection side, scanned text and image platforms, satellite remote sensing systems, and the World Wide Web have flooded us with a tremendous amount of data. This explosive growth has generated an even more urgent need for new techniques and automated tools that can help us transform this data into useful information and knowledge. Like the first edition, voted the most popular data mining book by KD Nuggets readers, this book explores concepts and techniques for the discovery of patterns hidden in large data sets, focusing on issues relating to their feasibility, usefulness, effectiveness, and scalability. However, since the publication of the first edition, great progress has been made in the development of new data mining methods, systems, and applications. This new edition substantially enhances the first edition, and new chapters have been added to address recent developments on mining complex types of data? including stream data, sequence data, graph structured data, social network data, and multi-relational data.\"\n}"
//    //      println(dealJsonString(jsonStr1))
//    //      println(parseAuthors("{\n      \"name\": \"jiawei han\",\n      \"org\": \"department of computer science university of illinois at urbana champaign\"\n    },\n    {\n      \"name\": \"micheline kamber\",\n      \"org\": \"department of computer science university of illinois at urbana champaign\"\n    },\n    {\n      \"name\": \"jian pei\",\n      \"org\": \"department of computer science university of illinois at urbana champaign\"\n    }"))
//    //    println(parseAuthors(getAuthorsString(jsonStr1)))
//    //      "{\"name\":\"abc\"}"
//    val jsonString = "123\"authors\":[123]"
//    var rtn = jsonString.substring(jsonString.indexOf("\"authors\"") + 9).indexOf(":[")
//    val c = parseAuthors( "{\"name\":\"abc\"}")
//    println(rtn)
//    println("INSERT INTO [Aminer].[dbo].[t_mag_papers] (id,title,venue,authors,year,keywords,fos,n_citation,page_start,page_end,doc_type,lang,publisher,volume,issue,issn,isbn,doi,url,pdf,abstract,reference,authorName,authorOrg) VALUES ('000000b8-7f59-49ad-b9bc-e92aa858fc37','EFL learners' use of online reading strategies and comprehension of texts: An exploratory study','Computers in Education','[  {    \"name\": \"Hsin-chou Huang\",    \"org\": \"Department of Applied English, St. John's University, 499, Tamkin Road, Section , Tamsui 251, Taiwan, ROC#TAB#\"  },  {    \"name\": \"Chiou-lan Chern\",    \"org\": \"Department of English, National Taiwan Normal University, 162, He-ping East Road, Section , Taipei 106, Taiwan, ROC#TAB#\"  },  {    \"name\": \"Chih-cheng Lin\",    \"org\": \"Department of English, National Taiwan Normal University, 162, He-ping East Road, Section , Taipei 106, Taiwan, ROC#TAB#\"  }]','2009','reading programs;reading strategies;teaching learning strategies;reading comprehension;pedagogical issues;data analysis;online courses;improving classroom teaching;post secondary education;exploratory study;college english;english second language;majors students','Natural language processing;Social science;Computer Science;Multimedia;Sociology;Reciprocal teaching;Data analysis;Exploratory research;Pedagogy','103','13','26','Journal','en@@@zh_cht','Elsevier. 6277 Sea Harbor Drive, Orlando, FL 32887-4800. Tel: 877-839-7126; Tel: 407-345-4020; Fax: 407-363-1354; e-mail: usjcs@elsevier.com; Web site: http://www.elsevier.com','52','1','','','10.1016/j.compedu.2008.06.003','https://www.learntechlib.org/p/67123/share;http://ntour.ntou.edu.tw:8080/ir/handle/987654321/23022?locale=en-US;http://eric.ed.gov/?id=EJ819454;http://www.editlib.org/p/67123/share;https://www.learntechlib.org/p/67123/;http://www.sciencedirect.com/science/article/pii/S0360131508000936;http://dl.acm.org/citation.cfm?id=1465358;http://ntour.ntou.edu.tw:8080/ir/handle/987654321/23022;https://eric.ed.gov/?id=EJ819454;http://dblp.uni-trier.de/db/journals/ce/ce52.html#HuangCL09;http://dl.acm.org/citation.cfm?id=1464531.1465358;http://dx.doi.org/10.1016/j.compedu.2008.06.003;http://www.editlib.org/p/67123;http://linkinghub.elsevier.com/retrieve/pii/S0360131508000936;https://doi.org/10.1016/j.compedu.2008.06.003','','This study investigated EFL learners' online reading strategies and the effects of strategy use on comprehension. To fulfill the purposes of this study, a Web-based reading program, English Reading Online, was created. Thirty applied English majors, divided into a high group and a low group based on their proficiency levels, were asked to read four authentic online texts; two were appropriate to the students' level of proficiency, and two were more difficult. Results from data analysis showed that the use of support strategies dominated the strategy use and contributed to most of the comprehension gains, but an exclusive dependence on support strategies did not successfully predict the increase in scores on main ideas and details when the students were reading more challenging texts. On the whole, the use of global strategies significantly contributed to better comprehension, especially for low proficiency students.','0299a529-2c14-4b4d-8767-cbe46e0f364a;0944c6dc-0875-452e-821a-636da2045b67;0aa0ffd3-ce9b-4a53-9751-b958aebae940;123014ca-20ef-4835-a733-2a1516871574;1f96328c-2ef8-4862-88f1-8ec7267b5f3a;21211985-3c32-4125-bce0-649a98fe3d98;214bced0-4719-4841-b18e-b9f160905f15;21d1db87-ec87-4694-8fa2-0f51bf4af988;3372ec17-ccf6-479a-8db6-05751b6c1430;3ac06386-baa2-4e19-847a-7eaf05197ab9;455a9651-54e7-4692-88af-35d436ea7e0a;4876dda4-803a-4886-8a8f-6b2a57369e72;509e8f12-c190-46cf-990e-1d38d50a1120;51e4bfe4-d62d-4e51-8fb6-dc305df23857;5516349f-1513-403d-b816-ce07c798ebc1;59bb8c62-1fb4-477e-b731-ccde521d9b28;623828da-a1b3-443d-a80e-b05d87f4ccff;6408c691-0236-40ed-b5bf-ee4fe359776f;79043465-3757-42c5-ab68-16cc5246d1a8;83716c19-605a-4bb8-ad8d-f1bb42b7454b;84e85701-dfd6-439e-a253-c0d24567f658;90076db3-8c50-4724-a618-c3bc830e86a1;92b57454-2ba8-4981-97b1-1a29a65e8860;aa665d64-7faa-405d-ade8-ccfaf8863fbb;aab8f48d-49ff-40ad-9b33-0652059fcac0;afc7da94-de77-4670-9a09-137772a653fb;b4a71877-3d70-4078-aba4-a484d2756081;c89e29e5-695c-4543-b25b-13a1c618a62a;c99273ed-e262-4a87-85a4-153dea2a7a2b;cb6c8c99-0acd-492b-9b69-30f90b8e6e8c;cd6dc174-e902-4dcb-a561-cd53ab39c967;d229ae02-4c4a-4396-8355-6b365f621447;dca42d6f-8ae1-4e20-812b-cda008b7127f;ec66681e-1fc4-4ac8-ba1f-3f29b868e452;f02ce13f-0fed-40e6-b964-54323ee94c44;fa87fa05-f2be-4963-b9e6-4b638196c148;faacad92-4db8-4126-aec0-0c1326f5736b;ffda715d-d7d2-4938-834e-56684ccdf91f','Hsin-chou Huang;Chiou-lan Chern;Chih-cheng Lin','Department of Applied English, St. John's University, 499, Tamkin Road, Section , Tamsui 251, Taiwan, ROC#TAB#;Department of English, National Taiwan Normal University, 162, He-ping East Road, Section , Taipei 106, Taiwan, ROC#TAB#;Department of English, National Taiwan Normal University, 162, He-ping East Road, Section , Taipei 106, Taiwan, ROC#TAB#')".replace("'",""))
println("\ud55c\uad6d\uae30\uc0c1\ud559\ud68c")
  }


}
