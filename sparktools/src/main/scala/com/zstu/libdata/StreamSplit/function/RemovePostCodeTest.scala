package com.zstu.libdata.StreamSplit.function

import org.apache.spark.sql.hive.HiveContext

/**
  * Created by Administrator on 2017/10/18 0018.
  */
class RemovePostCodeTest(postArray:Array[String]) {
  def removePostCode(institute: String): String ={
    if(institute == null) null
    else {
      var result = institute
      for (postCode <- postArray){
        if(result.indexOf(postCode) > 0){
          result = result.replace(postCode + ",","").replace(postCode,"")
        }
      }
      result
    }

  }
}
