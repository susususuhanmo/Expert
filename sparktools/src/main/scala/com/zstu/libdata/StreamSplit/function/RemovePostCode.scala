package com.zstu.libdata.StreamSplit.function

import org.apache.spark.sql.hive.HiveContext

/**
  * Created by Administrator on 2017/10/13 0013.
  */
class RemovePostCode(hiveContext: HiveContext) {
val postArray = getData.getPostArray(hiveContext)
  def removePostCode(institute: String): String ={
    var result = institute
    for (postCode <- postArray){
      if(result.indexOf(postCode) > 0){
        result = result.replace(postCode + ",","").replace(postCode,"")
      }
    }
    result
  }

}
