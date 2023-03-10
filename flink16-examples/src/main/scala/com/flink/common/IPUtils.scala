package com.flink.common

// ip转地址封装类

object IPUtils {

  def binarySearch(ipNum:Long, ipArr:Array[(String,Long,Long)]):Int = {
    var start = 0
    var end = ipArr.length - 1

    while(start <= end) {
      //获取中间的索引位置
      val mid = (start + end) / 2
      val startIP:Long = ipArr(mid)._2
      val endIP:Long = ipArr(mid)._3
      //判断
      if(ipNum >= startIP && ipNum <= endIP) return mid
      else if(ipNum < startIP) end = mid - 1
      else start = mid + 1
    }
    return -1
  }

  //将ip的字符转换为一个地址
  def ip2Address(ipStr:String, ipArr:Array[(String,Long,Long)]):String = {
    //将目标ip转化为数字
    val ipNum:Long = ip2Long(ipStr)
    //二分查找法
    val index = binarySearch(ipNum,ipArr)
    //获取地址
    if(index != -1)return ipArr(index)._1
    else return ""
  }

  //将ip字符串形式转换为ip数字的表示形式
  def ip2Long(ip: String):Long = {
    //切割字符串的ip
    val fields: Array[String] = ip.split("\\.")
    // 遍历数组，就能获取到每个ip的数字
    var ipNum = 0L
    fields.foreach(field => {
      ipNum = field.toLong | ipNum << 8
    })
    ipNum
  }
}
