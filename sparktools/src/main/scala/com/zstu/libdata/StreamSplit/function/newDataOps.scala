package com.zstu.libdata.StreamSplit.function

import com.zstu.libdata.StreamSplit.kafka.cnkiClean
import com.zstu.libdata.StreamSplit.splitAuthor.splitAuthorFunction.splitRdd
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by Administrator on 2017/6/16 0016.
  */
object newDataOps {

  //  /**
  //    * 处理新数据
  //    * 1、整合完整数据写入Log表，更新内存中的待匹配数据
  //    * 2、拆分作者并匹配，更新内存中的作者数据。
  //    *
  //    * @param joinedGroupedRdd     匹配Join，group过的数据
  //    * @param fullInputRdd         完整输入数据
  //    * @param sourceCoreRdd        核心数据判断是否为核心期刊
  //    * @param journalMagSourceRdd  数据来源
  //    * @param simplifiedJournalRdd 简化数据
  //    * @param types                来源
  //    * @param authorRdd            作者数据
  //    * @param CLCRdd               学科数据
  //    * @param hiveContext          hiveContext
  //    * @param forSplitRdd          待拆分数据
  //    * @return 更新后的journal数据和作者数据
  //    */
  //  def dealNewData(joinedGroupedRdd: RDD[(String, Iterable[((String, String, String, String, String, String), Option[(String, String, String, String, String, String)])])],
  //                  fullInputRdd: RDD[(String, (String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String))],
  //                  sourceCoreRdd: RDD[(String, String)],
  //                  journalMagSourceRdd: RDD[(String, String)],
  //                  simplifiedJournalRdd: RDD[(String, (String, String, String, String, String, String))],
  //                  types: Int,
  //                  authorRdd: RDD[((String, String), Any)],
  //                  CLCRdd: (RDD[(String, (String, String))]),
  //                  hiveContext: HiveContext
  //                  , forSplitRdd: RDD[(String, ((String, String), (String, String, String, String, String)))],
  //                  universityData: DataFrame)
  //  : (RDD[(String, (String, String, String, String, String, String))], RDD[((String, String), Any)])
  //  = {
  //    /** ****** 相似度小于90的处理开始 *******/
  //    val rdd_kafka_result_notmatch = joinedGroupedRdd
  //      .filter(f => commonOps.filterDisMatchData(f._2))
  //      .map(f => (f._1, f._2.take(1).toList.head._1))
  //    //入库(完整字段）
  //    if (rdd_kafka_result_notmatch.first() != null) {
  //      val rdd_kafka_newdata_source = fullInputRdd.join(rdd_kafka_result_notmatch).distinct()
  //      val rdd_kafka__newdata_source_insert_map = rdd_kafka_newdata_source.map(f => (f._2._1._12, (f._2._1._1, f._2._1._2, f._2._1._3, f._2._1._4, f._2._1._5, f._2._1._6, f._2._1._7, f._2._1._8, f._2._1._9, f._2._1._10, f._2._1._11, f._2._1._12, f._2._1._13, f._2._1._14, f._2._1._15, f._2._1._16, f._2._2)))
  //      val rdd_kafka_newdata_source_insert_map2 = rdd_kafka__newdata_source_insert_map.leftOuterJoin(sourceCoreRdd)
  //      val rdd_kafka_newdata_source_insert_map3 = rdd_kafka_newdata_source_insert_map2.leftOuterJoin(journalMagSourceRdd)
  //      val rdd_kafka__newdata_source_insert_final = rdd_kafka_newdata_source_insert_map3.map(f = f => {
  //        var iscore = 0
  //        var journaldatasource = ""
  //        if (f._2._1._2.isEmpty)
  //          iscore = 0
  //        else
  //          iscore = 1
  //        if (f._2._2.isEmpty)
  //          journaldatasource = ""
  //        else
  //          journaldatasource = f._2._2.get
  //        (f._2._1._1._1, f._2._1._1._2, f._2._1._1._3, f._2._1._1._4, f._2._1._1._5, f._2._1._1._6, f._2._1._1._7, f._2._1._1._8, f._2._1._1._9, f._2._1._1._10, f._2._1._1._11, f._2._1._1._12, f._2._1._1._13, f._2._1._1._14, iscore, 1, "", journaldatasource, f._2._1._1._15, f._2._1._1._16)
  //      })
  //      //      logUtil("------------rdd_kafka__newdata_source_insert_final：" + rdd_kafka__newdata_source_insert_final.first().toString() + "---------------")
  //
  //      val rdd_kafka_newdata_source_insert_to_memory = rdd_kafka__newdata_source_insert_final.map(f => (commonOps.getJournalKay(f._2, f._12), (f._2, f._12, f._4, f._1, f._9, f._11)))
  //
  //      //todo 此处为新数据写入log表。测试暂时注释掉。20170601 17：18
  //                  rdd_kafka__newdata_source_insert_final.foreach(f => {
  //                    var conn: Connection = null
  //                    var stmt: PreparedStatement = null
  //
  //                    conn = DriverManager.getConnection(commonOps.sqlUrl, commonOps.userName, commonOps.passWord);
  //                    val insertSQL = "insert into t_JournalLog(id,title,titleAlt,creator,creatorAlt,creatorAll,keyword,keywordAlt,institute,instituteAll,year,journal,issue,url,isCore,operater,operateTime,source,isCheck,otherId,volume,page,datasource,abstract,abstractAlt)" +
  //                      "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,getdate()," + types + ",0,?,?,?,?,?,?)"
  //                    stmt = conn.prepareStatement(insertSQL)
  //                    commonOps.setData4newdata(stmt, f, types)
  //                    stmt.execute()
  //
  //                  })
  //
  //
  //      val newId: RDD[(String, Null)] = rdd_kafka__newdata_source_insert_final.map(value => (value._1, null))
  //      logUtil("新id数据" + newId.first())
  //      logUtil("forSplitRdd" + forSplitRdd.first())
  //      val inputRdd: RDD[((String, String), Any)]
  //      = forSplitRdd.join(newId).map(value => value._2._1)
  //      logUtil("join得到带拆分新数据" + inputRdd.count())
  //
  //      (simplifiedJournalRdd.union(rdd_kafka_newdata_source_insert_to_memory)
  //        , splitRddNew(inputRdd, authorRdd, CLCRdd,universityData, hiveContext))
  //
  //    }
  //    else (simplifiedJournalRdd, authorRdd)
  //
  //    /** ****** 相似度小于90的处理结束 *******/
  //  }


  //  def dealNewDataPage(joinedGroupedRdd: RDD[(String, Iterable[((String, String, String, String, String, String), Option[(String, String, String, String, String, String)])])],
  //                      fullInputRdd: RDD[(String, (String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String))],
  //                      sourceCoreRdd: RDD[(String, String)],
  //                      journalMagSourceRdd: RDD[(String, String)],
  //                      simplifiedJournalRdd: RDD[(String, (String, String, String, String, String, String))],
  //                      types: Int,
  //                      authorRdd: RDD[((String, String), Any)],
  //                      CLCRdd: (RDD[(String, (String, String))]),
  //                      hiveContext: HiveContext
  //                      , forSplitRdd: RDD[(String, ((String, String), (String, String, String, String, String)))],
  //                      universityData: DataFrame)
  //  : (RDD[(String, (String, String, String, String, String, String))], RDD[((String, String), Any)], RDD[(String, (String, String))])
  //
  //  = {
  //    /** ****** 相似度小于90的处理开始 *******/
  //    val  rdd_kafka_result_notmatch = joinedGroupedRdd.map(f => (f._1, f._2.take(1).toList.apply(0)._1))
  //
  //
  //    //入库(完整字段）
  //    if (rdd_kafka_result_notmatch.first() != null) {
  //
  //
  //
  //      val rdd_kafka_newdata_source = fullInputRdd.join(rdd_kafka_result_notmatch).distinct()
  //      val rdd_kafka__newdata_source_insert_map = rdd_kafka_newdata_source.map(f => (f._2._1._12, (f._2._1._1, f._2._1._2, f._2._1._3, f._2._1._4, f._2._1._5, f._2._1._6, f._2._1._7, f._2._1._8, f._2._1._9, f._2._1._10, f._2._1._11, f._2._1._12, f._2._1._13, f._2._1._14, f._2._1._15, f._2._1._16, f._2._2, f._2._1._17)))
  //      val rdd_kafka_newdata_source_insert_map2 = rdd_kafka__newdata_source_insert_map.leftOuterJoin(sourceCoreRdd)
  //      val rdd_kafka_newdata_source_insert_map3 = rdd_kafka_newdata_source_insert_map2.leftOuterJoin(journalMagSourceRdd)
  //
  //
  //      val rdd_kafka__newdata_source_insert_final
  //      : RDD[(String, String, String, String, String, String, String, String, String, String, String, String, String, String, Int, Int, String, String, String, String, String)] = rdd_kafka_newdata_source_insert_map3.map(f => {
  //        var iscore = 0
  //        var journaldatasource = ""
  //        if (f._2._1._2.isEmpty)
  //          iscore = 0
  //        else
  //          iscore = 1
  //        if (f._2._2.isEmpty)
  //          journaldatasource = ""
  //        else
  //          journaldatasource = f._2._2.get
  //        (f._2._1._1._1, f._2._1._1._2, f._2._1._1._3, f._2._1._1._4, f._2._1._1._5, f._2._1._1._6, f._2._1._1._7, f._2._1._1._8, f._2._1._1._9, f._2._1._1._10, f._2._1._1._11, f._2._1._1._12, f._2._1._1._13, f._2._1._1._14, iscore, 1, "", journaldatasource, f._2._1._1._15, f._2._1._1._16,f._2._1._1._18)
  //      })
  //      //      logUtil("------------rdd_kafka__newdata_source_insert_final：" + rdd_kafka__newdata_source_insert_final.first().toString() + "---------------")
  //
  //      val rdd_kafka_newdata_source_insert_to_memory = rdd_kafka__newdata_source_insert_final.map(f => (commonOps.getJournalKay(f._2, f._12), (f._2, f._12, f._4, f._1, f._9, f._11)))
  //
  //
  //
  //
  //      val newId: RDD[(String, Null)] = rdd_kafka__newdata_source_insert_final.map(value => (value._1, null))
  //      logUtil("新id数据" + newId.first())
  //      logUtil("forSplitRdd" + forSplitRdd.first())
  //      val inputRdd: RDD[((String, String), Any)]
  //      = forSplitRdd.join(newId).map(value => value._2._1)
  //      logUtil("join得到带拆分新数据" + inputRdd.count())
  //
  //      val updatesimplifiedJournalRdd=  simplifiedJournalRdd.union(rdd_kafka_newdata_source_insert_to_memory)
  //      val (newAuthor,newjournalLogRdd) = splitRddNew(inputRdd, authorRdd, CLCRdd,universityData, hiveContext)
  //
  //
  //      val rdd_kafka__newdata_source_insert_final2 = rdd_kafka__newdata_source_insert_final
  //        .map(
  //          value => (value._1,(value._1,value._2,value._3,value._4,value._5,value._6,value._7
  //            ,value._8,value._9,value._10,value._11,value._12,value._13,value._14,value._15
  //            ,value._16,value._17,value._18,value._19,value._20,value._20,value._21))
  //        )
  //        .leftOuterJoin(newjournalLogRdd)
  //      def getOption(opt: Option[(String, String)]) ={
  //        opt.getOrElse((null,null))._1
  //      }
  //      val rdd_kafka__newdata_source_insert_final3
  //      : RDD[(String, String, String, String, String, String
  //        , String, String, String, String, String, String, String
  //        , String, Int, Int, String, String, String, String, String
  //        , Option[(String, String)])]
  //      = rdd_kafka__newdata_source_insert_final2.map(value =>
  //        (
  //          cutStr(value._2._1._1,4000),cutStr(value._2._1._2,4000),cutStr(value._2._1._3,4000),cutStr(value._2._1._4,4000),cutStr(value._2._1._5,4000),cutStr(value._2._1._6,4000),cutStr(value._2._1._7
  //          ,4000),cutStr(value._2._1._8,4000),cutStr(value._2._1._9,4000),cutStr(value._2._1._10,4000),cutStr(value._2._1._11,4000),cutStr(value._2._1._12,4000),cutStr(value._2._1._13,4000),cutStr(value._2._1._14,4000),value._2._1._15
  //          ,value._2._1._16,cutStr(value._2._1._17,4000),cutStr(value._2._1._18,4000),cutStr(value._2._1._19,4000),cutStr(value._2._1._20,4000),cutStr(value._2._1._21,4000),value._2._2
  //
  //
  //        )
  //      )
  //
  //
  //      //      todo 此处为新数据写入log表。测试暂时注释掉。20170601 17：18
  //      rdd_kafka__newdata_source_insert_final3.foreach(f => {
  //        var conn: Connection = null
  //        var stmt: PreparedStatement = null
  //
  //        conn = DriverManager.getConnection(commonOps.sqlUrl, commonOps.userName, commonOps.passWord);
  //        val insertSQL = "insert into t_JournalLog(id,title,titleAlt,creator,creatorAlt,creatorAll,keyword,keywordAlt,institute,instituteAll,year,journal,issue,url,isCore,operater,operateTime,source,isCheck,otherId,volume,page,datasource,abstract,abstractAlt,candidateResources,subject)" +
  //          "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,getdate()," + types + ",0,?,?,?,?,?,?,?,?)"
  //        stmt = conn.prepareStatement(insertSQL)
  //        commonOps.setData4newdataPage(stmt, f, types)
  //        stmt.execute()
  //
  //      })
  //
  //      (updatesimplifiedJournalRdd,newAuthor,newjournalLogRdd)
  //    }
  //    else (simplifiedJournalRdd, authorRdd,null)
  //
  //    /** ****** 相似度小于90的处理结束 *******/
  //  }


  //  def dealNewData0621(joinedGroupedRdd: RDD[(String, Iterable[((String, String, String, String, String, String), Option[(String, String, String, String, String, String)])])],
  //                      fullInputRdd: RDD[(String, (String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String))],
  //                      sourceCoreRdd: RDD[(String, String)],
  //                      journalMagSourceRdd: RDD[(String, String)],
  //                      simplifiedJournalRdd: RDD[(String, (String, String, String, String, String, String))],
  //                      types: Int,
  //                      authorRdd: RDD[((String, String), Any)],
  //                      CLCRdd: (RDD[(String, (String, String))]),
  //                      hiveContext: HiveContext
  //                      , forSplitRdd: RDD[(String, ((String, String), (String, String, String, String, String)))],
  //                      universityData: DataFrame)
  //  : (RDD[(String, (String, String, String, String, String, String))], RDD[((String, String), Any)], RDD[(String, (String, String))])
  //
  //  = {
  //    /** ****** 相似度小于90的处理开始 *******/
  //    val  rdd_kafka_result_notmatch = joinedGroupedRdd.map(f => (f._1, f._2.take(1).toList.apply(0)._1))
  //
  //
  //    //入库(完整字段）
  //    if (rdd_kafka_result_notmatch.first() != null) {
  //
  //
  //
  //      val rdd_kafka_newdata_source = fullInputRdd.join(rdd_kafka_result_notmatch).distinct()
  //
  //      val rdd_kafka__newdata_source_insert_map = rdd_kafka_newdata_source.map(f => (f._2._1._12, (f._2._1._1, f._2._1._2, f._2._1._3, f._2._1._4, f._2._1._5, f._2._1._6, f._2._1._7, f._2._1._8, f._2._1._9, f._2._1._10, f._2._1._11, f._2._1._12, f._2._1._13, f._2._1._14, f._2._1._15,f._2._1._16,f._2._1._17,f._2._1._18,f._2._2)))
  //      val rdd_kafka_newdata_source_imsert_map2 = rdd_kafka__newdata_source_insert_map.leftOuterJoin(sourceCoreRdd)
  //      val rdd_kafka_newdata_source_imsert_map3 = rdd_kafka_newdata_source_imsert_map2.leftOuterJoin(journalMagSourceRdd)
  //
  //
  //      //样例：(F2BB2E10-4C2A-E711-AECF-0050569B7A51,第33届国际地理大会在京圆满闭幕,,,,,中国地理学会;国际;中国科学院;地理科学;科学技术;资源,,,,2016,资源导刊,2016年第0卷第15期 5-5页,共1页,http://lib.cqvip.com/qk/96616B/201615/670472909.html,0,1,第33届国际地理大会在京圆满闭幕)-
  //      val rdd_kafka__newdata_source_insert_final
  //      = rdd_kafka_newdata_source_imsert_map3.map(f => {
  //        var iscore = 0
  //        var journaldatasource = ""
  //        if (f._2._1._2.isEmpty)
  //          iscore = 0
  //        else
  //          iscore = 1
  //        if(f._2._2.isEmpty)
  //          journaldatasource = ""
  //        else
  //          journaldatasource =f._2._2.get
  //        (f._2._1._1._1, f._2._1._1._2, f._2._1._1._3, f._2._1._1._4, f._2._1._1._5, f._2._1._1._6, f._2._1._1._7, f._2._1._1._8, f._2._1._1._9, f._2._1._1._10, f._2._1._1._11, f._2._1._1._12, f._2._1._1._13, f._2._1._1._14, iscore, 1, "",journaldatasource, f._2._1._1._15, f._2._1._1._16, f._2._1._1._17, f._2._1._1._18)
  //      })
  //
  //      //      rdd_kafka__newdata_source_insert_final
  //      //        .foreach(f => {
  //      //          var conn: Connection = null
  //      //          var stmt: PreparedStatement = null
  //      //          try {
  //      //            conn = DriverManager.getConnection(commonClean.sqlUrl, commonClean.userName, commonClean.passWord)
  //      //            //插入数据库 方法1
  //      //            val insertSQL = "insert into t_JournalLog(id,title,titleAlt,creator,creatorAlt,creatorAll,keyword,keywordAlt,institute,instituteAll,year,journal,issue,url,isCore,operater,operateTime,source,isCheck,otherId,volume,page,datasource,abstract,abstractAlt,classifications)" +
  //      //              "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,getdate(),"+types+",0,?,?,?,?,?,?,?)"
  //      //            stmt = conn.prepareStatement(insertSQL)
  //      //            commonClean.setData4newdata(stmt, f, types)
  //      //
  //      //            stmt.execute()
  //      //          }
  //      //        })
  //
  //
  //
  //      val newId: RDD[(String, Null)] = rdd_kafka__newdata_source_insert_final.filter(value =>value._15 == 1)
  //        .map(value => (value._1, null))
  //
  //
  //      val inputRdd: RDD[((String, String), Any)]
  //      = forSplitRdd.join(newId).map(value => value._2._1)
  //
  //
  //      val (newAuthor,newjournalLogRdd) = splitRddNew(inputRdd, authorRdd, CLCRdd,universityData, hiveContext)
  //
  //
  //      val rdd_kafka__newdata_source_insert_final2 = rdd_kafka__newdata_source_insert_final
  //        .map(
  //          value => (value._1,(value._1,value._2,value._3,value._4,value._5,value._6,value._7
  //            ,value._8,value._9,value._10,value._11,value._12,value._13,value._14,value._15
  //            ,value._16,value._17,value._18,value._19,value._20,value._21,value._22))
  //        )
  //        .leftOuterJoin(newjournalLogRdd)
  //      def getOption(opt: Option[(String, String)]) ={
  //        opt.getOrElse((null,null))._1
  //      }
  //      val rdd_kafka__newdata_source_insert_final3
  //      : RDD[(String, String, String, String, String, String
  //        , String, String, String, String, String, String, String
  //        , String, Int, Int, String, String, String, String, String
  //        , Option[(String, String)])]
  //      = rdd_kafka__newdata_source_insert_final2.map(value =>
  //        (
  //
  //          value._2._1._1,value._2._1._2,value._2._1._3,value._2._1._4,value._2._1._5,value._2._1._6,value._2._1._7
  //          ,value._2._1._8,value._2._1._9,value._2._1._10,value._2._1._11,value._2._1._12,value._2._1._13,value._2._1._14,value._2._1._15
  //          ,value._2._1._16,value._2._1._17,cutStr(value._2._1._21,50),value._2._1._19,value._2._1._20,cutStr(value._2._1._22,50),value._2._2
  //        )
  //      )
  //
  //
  //
  //
  //
  //
  //
  //
  //
  //
  //
  //
  //
  //      //      todo 此处为新数据写入log表。测试暂时注释掉。20170601 17：18
  //      rdd_kafka__newdata_source_insert_final3.foreach(f => {
  //        var conn: Connection = null
  //        var stmt: PreparedStatement = null
  //
  //        conn = DriverManager.getConnection(commonOps.sqlUrl, commonOps.userName, commonOps.passWord);
  //        val insertSQL = "insert into t_JournalLog(id,title,titleAlt,creator,creatorAlt,creatorAll,keyword,keywordAlt,institute,instituteAll,year,journal,issue,url,isCore,operater,operateTime,source,isCheck,otherId,volume,page,classifications,abstract,abstractAlt,candidateResources,subject)" +
  //          "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,getdate()," + types + ",0,?,?,?,?,?,?,?,?)" //24
  //        stmt = conn.prepareStatement(insertSQL)
  //        commonOps.setData4newdataPage(stmt, f, types)
  //        stmt.execute()
  //
  //      })
  //
  //
  //
  //
  //
  //
  //
  //
  //
  //
  //
  //
  //
  //
  //
  //
  //
  //
  //
  //
  //
  //
  //
  //
  //
  //
  //
  //
  //
  //
  //
  //      (null,newAuthor,newjournalLogRdd)
  //    }
  //    else (simplifiedJournalRdd, authorRdd,null)
  //
  //    /** ****** 相似度小于90的处理结束 *******/
  //  }
  case class journalCoreJudge(journalName: String, isCore: Int)

  case class noMatchData(idNoMatch: String)

  case class operateAndSource(operater: Int, source: Int)

  def dealNewData0623(fullInputData: DataFrame,
                      journalMagSourceRdd: RDD[(String, String)],
                      simplifiedJournalRdd: RDD[(String, (String, String, String, String, String, String, String, String))],
                      types: Int,
                      inputJoinJournalRdd: RDD[(String, ((String, String, String, String, String, String, String, String), Option[(String, String, String, String, String, String, String, String)]))],
                      authorRdd: RDD[((String, String), Any)],
                      CLCRdd: (RDD[(String, (String, String))]),
                      hiveContext: HiveContext
                      , forSplitRdd: RDD[(String, ((String, String), (String, String, String, String, String)))],
                      universityData: DataFrame): RDD[((String, String), Any)]

  = {




    // fullData 当前拥有列
    // (id, title, titleAlt, creator, creatorAlt, creatorAll, keyWord,
    // keyWordAlt, institute, instituteAll, year, journal, issue
    // , url,abstract,abstract_alt,page,classification)

    //    (id,title,titleAlt,creator,creatorAlt,creatorAll,keyword,
    // keywordAlt,institute,instituteAll,year,journal,issue// ,url
    // ,operater,source
    // page,classifications,abstract,abstractAlt
    // ,candidateResources,subject)



    printLog.logUtil("InputData" + fullInputData.count())

    /** ****** 相似度小于90的处理开始 *******/
    //    val rdd_kafka_result_notmatch: RDD[(String, (String, String, String, String, String, String))] = joinedGroupedRdd.map(f => (f._1, f._2.take(1).toList.apply(0)._1))

    val matchedId = inputJoinJournalRdd
      .filter(f => commonOps.getDisMatchRecord(f._2)).map(value => (value._1, "")).distinct
    printLog.logUtil("matchCount" + matchedId.count())
    printLog.logUtil("matchCount" + matchedId.count())

    val rdd_kafka_result_notmatch = inputJoinJournalRdd.map(value => value._1)
      .distinct.map(value => (value, ""))
      .leftOuterJoin(matchedId).filter(value => value._2._2.orNull == null)
      .map(value => value._1)
    if (rdd_kafka_result_notmatch.count() == 0) return authorRdd

    printLog.logUtil("noMatchRdd" + rdd_kafka_result_notmatch.count())
    val resultNomatchData = hiveContext.createDataFrame(rdd_kafka_result_notmatch.map(
      value => noMatchData(value)
    ))
    //入库(完整字段）

    val magData = getData.getMagData(hiveContext: HiveContext)
//    val coreData = getData.getCoreData(hiveContext: HiveContext)

    val noMatchFullData = fullInputData.join(resultNomatchData,
      fullInputData("id") === resultNomatchData("idNoMatch"))
      .drop("idNoMatch")
    printLog.logUtil("noMatchRddfullData" + noMatchFullData.count())


    //    val noMatchFullDataWithCore = noMatchFullData.join(coreData,
    //      noMatchFullData("journal") === coreData("journalName"), "left")
    //      .drop("journalName")


    //    noMatchFullData.registerTempTable("noMatchFullData")
    //    hiveContext.udf.register("judgeIsCore", (str: String) =>if(str !="" && str !=null) isCore.isCore(str) else null)
    //    val noMatchFullDataWithCore = hiveContext.sql("" +
    //      "select *,judgeIsCore(journal) as isCore from noMatchFullData")

//    val journalRdd = noMatchFullData.map(row => row.getString(row.fieldIndex("journal"))).distinct()
//
//    val journalCoreData = hiveContext.createDataFrame(journalRdd.map(value => journalCoreJudge(value, isCore(value))))
//
//    val noMatchFullDataWithCore = noMatchFullData.join(journalCoreData,
//      noMatchFullData("journal") === journalCoreData("journalName"), "left")
//      .drop("journalName")
//
//    val noMatchFullDataWithMag = noMatchFullDataWithCore.join(magData,
//      noMatchFullDataWithCore("journal") === magData("journalName"), "left")
//      .drop("journalName")


    val newId: RDD[(String, Null)] = noMatchFullData.filter("isCore = 1").map(row =>
      (row.getString(row.fieldIndex("id")), null))


    val inputRdd: RDD[((String, String), Any)]
    = forSplitRdd.join(newId).map(value => value._2._1)
    val yearData = fullInputData.select("id", "year")

    val (newAuthor, newjournalLogRdd) = splitRdd(inputRdd, authorRdd, CLCRdd, universityData, yearData, hiveContext)


    val cadidateData = hiveContext.createDataFrame(newjournalLogRdd)


    //    idCandidate
    val noMatchDataWithSubject = noMatchFullData.join(cadidateData,
      noMatchFullData("id") === cadidateData("idCandidate"), "left")
      .drop("idCandidate")
    printLog.logUtil("noMatchDataWithSubject" + noMatchDataWithSubject.count())

    //    case class operateAndSource(operater:String,source:String)
    val operateSourceData = hiveContext.createDataFrame(Array(operateAndSource(1, types)))
    val resultData = noMatchDataWithSubject.join(operateSourceData).cache


    //    WriteData.writeDataStream("t_JournalLog",resultData)

    WriteData.writeDataLog("tmp_JournalLog", resultData)
//    WriteData.writeDataLog("test_JournalLog", resultData)






    //    WriteData.writeDataDiscoveryV2("t_JournalLog",resultData
    //      .drop("candidateResources").drop("subject").filter("isCore = 1"))

    printLog.logUtil("resultData" + resultData.count())
    authorRdd


    /** ****** 相似度小于90的处理结束 *******/
  }

}



