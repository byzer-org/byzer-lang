package streaming.core.strategy

import java.util

/**
 * 12/17/14 WilliamZhu(allwefantasy@gmail.com)
 */
trait DebugTrait {

  def putDebug(params: util.Map[Any, Any],key:String,value:AnyRef) = {
    putDebug2(params,this.toString,key,value)
  }

  def putDebug2(params: util.Map[Any, Any],classKey:String,key:String,value:AnyRef) = {

    if(debugEnable(params)){
      val debugInfo = params.get("_debug_").asInstanceOf[util.Map[String,AnyRef]]
      if(!debugInfo.containsKey(classKey)){
        debugInfo.put(classKey,new util.HashMap[String,AnyRef]())
      }
      debugInfo.get(classKey).asInstanceOf[util.Map[String,AnyRef]].put(key,value)
    }
  }

  def changeDebugData(stragetyParams: java.util.Map[Any, Any]): Map[Any, Any] = {

    val debugData = stragetyParams.get("_debug_").asInstanceOf[util.Map[Any, Any]]
    debugData.keySet().toArray.map{
      f=>
        val temp =  debugData.get(f).asInstanceOf[util.HashMap[String, AnyRef]]

        (f, temp.keySet().toArray.map(k=> (k, temp.get(k))).toMap)
    }.toMap
  }


  def debugEnable(params: util.Map[Any, Any]) = {
    params.containsKey("debug") && params.get("debug").asInstanceOf[String].toBoolean
  }
}
