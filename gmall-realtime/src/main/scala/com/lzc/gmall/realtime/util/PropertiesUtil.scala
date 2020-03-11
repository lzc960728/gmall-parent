package com.lzc.gmall.realtime.util

import java.io.InputStreamReader
import java.util.Properties

/**
 * @program: gmall-parent
 * @ClassName PropertiesUtil
 * @description:
 * @author: lzc
 * @create: 2020-03-09 13:52
 * @Version 1.0
 **/
object PropertiesUtil {
  def main(args: Array[String]): Unit = {
    val properties: Properties = PropertiesUtil.load("config.properties")

    println(properties.getProperty("kafka.broker.list"))
  }

  def load(propertieName:String): Properties ={
    val prop=new Properties();
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName) , "UTF-8"))
    prop
  }
}
