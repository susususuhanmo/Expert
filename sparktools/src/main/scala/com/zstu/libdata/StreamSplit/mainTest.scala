package com.zstu.libdata.StreamSplit

import java.io.PrintWriter

import com.zstu.libdata.StreamSplit.function.CommonTools._
import com.zstu.libdata.StreamSplit.function.ReadData
import com.zstu.libdata.StreamSplit.mainAll.latestTaskComplete
import org.apache.spark.sql.hive.HiveContext
import org.joda.time.DateTime

/**
  * Created by SuHanmo on 2017/7/1.
  * AppName: mainAll
  * Function:
  * Input Table:
  * Output Table:
  */
object mainTest {

  val logger = new PrintWriter("./allTest.txt")
  var day = 1
  var finishedCNKI = false
  var finishedVIP = false
  var finishedWF = false
  var dayCount = 0
  var todayRun = Array("WF", "CNKI", "VIP")

  var runMode = "All"
  var runTime = 19
  var runDataCount =5000

  def main(args: Array[String]) {


    val hiveContext = initSpark("mainALL")
    val inputPara = args.filter(a => a != "")
    inputPara.length match {
      case 1 =>{
        //只输入一个参数，其他参数为默认。
        //例如 ./main.sh ALL 即为循环运行，19点定时，每次5000
        //再如 ./main.sh CNKI 为单独运行CNKI,立刻运行，数量5000
        runMode = args(0).toUpperCase
        if(runMode != "ALL") runTime = -1
      }
      case 2 => {
        //只输入两个参数，其他参数为默认。
        //例如 ./main.sh ALL 17 即为循环运行，17点定时，每次5000
        //再如 ./main.sh CNKI 2000 为单独运行CNKI,立刻运行，数量2000
        runMode = args(0).toUpperCase
        if(runMode == "ALL")
        {
          runTime = args(1).toInt
        }else {
          runTime = -1
          runDataCount = args(1).toInt
        }
      }
      case 3 =>{
        //输入三个参数。
        //例如 ./main.sh ALL 17 1500 即为循环运行，17点定时，每次1500
        //再如 ./main.sh CNKI 12 2000 为单独运行CNKI,下一个12点开始，数量2000
        runMode = args(0).toUpperCase
        runTime = args(1).toInt
        runDataCount = args(2).toInt
      }
      case _ =>{
        logger.println("未输入参数按默认参数运行")
        logger.flush()
      }
    }

    logger.println("运行模式："+(if(runMode == "ALL") "循环定时运行" else "单独运行"+runMode))
    logger.println("开始时间："+(if(runTime >=0) runTime+":00" else "立刻运行"))
    logger.println("运行数量："+runDataCount)
    logger.flush()




    //            run("CNKI",hiveContext)
    //        run("VIP",hiveContext)
//    run("WF",hiveContext)

    /***********************以下内容为计时循环运行代码*****************************/
//    while (true) {
//      val runSource = refreshDate(hiveContext)
//      if (runSource != null) {
//        while (DateTime.now().hourOfDay().get() != runTime) {
//          Thread.sleep(1000 * 60 * 59)
//        }
//        if(latestTaskComplete(hiveContext)){
//          run(runSource, hiveContext)
//        }else{
//          //如果前一天任务未处理完成，今日不运行程序，第二天再接着运行。
//          if(dayCount == 0) dayCount =2 else dayCount -= 1
//        }
//      }
//    }
    /**********************************************************************/




  }

  def latestTaskComplete(hiveContext: HiveContext): Boolean = {
    val uncheckNum = ReadData.readDataLog("tmp_ExpertLog", hiveContext).filter("isCheck = 0").count()
    if (uncheckNum == 0) {
      logger.println("Latest Task has been Completed")
      logger.flush()
      true
    }
    else {
      logger.println("Latest Task has not been Completed")
      logger.flush()
      false
    }
  }

  def refreshDate(hiveContext: HiveContext): String = {
    val today = DateTime.now().dayOfWeek().get()
    if (today != day) {

      logger.println("DATE CHANGED!")
      logger.flush()
      //若到日期发生改变 改变count（0,1,2）循环
      if (dayCount == 2) dayCount = 0 else dayCount += 1
      //更新日期为今日
      day = today
      //返回应运行的数据源
      todayRun(dayCount)


    }
    //若日期没发生改变，则返回null不运行程序
    else {
      Thread.sleep(1000 * 60 * 60)
      null
    }
  }


  def run(source: String, hiveContext: HiveContext) = {
    logger.println("its time to run " + source + "!!!" + DateTime.now + "\r\n")
    logger.flush()
    try {
      source match {
        case "VIP" =>
          mainVIP.main(hiveContext: HiveContext)
          finishedVIP = true
        case "WF" =>
          mainWF.main(hiveContext: HiveContext)
          finishedWF = true
        case "CNKI" =>
          mainCNKI.main(hiveContext: HiveContext)
          finishedCNKI = true
        case _ =>
          logger.println("error:uncorrect source name!" + DateTime.now + "\r\n")
          logger.flush()
      }

    } catch {
      case ex: Exception =>
        logger.println("exception" + ex.getMessage + "\ntime:" + DateTime.now + "\r\n")
        logger.flush()
    }
  }

}
