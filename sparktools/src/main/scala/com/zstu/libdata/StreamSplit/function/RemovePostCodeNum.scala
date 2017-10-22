package com.zstu.libdata.StreamSplit.function

/**
  * Created by Administrator on 2017/10/18 0018.
  */
object RemovePostCodeNum {


  def removePostCode(str: String)={
    val reg1 = """\d{6},(?!\d)""".r
    val reg = """\d{6}(?!\d)""".r

    reg.replaceAllIn(reg1.replaceAllIn(str,""),"")
  }

  def main(args: Array[String]): Unit = {
println(removePostCode("112345先"))
  }

}
