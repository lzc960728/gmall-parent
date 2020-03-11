package com.lzc.gmall.realtime.bean

/**
 * @program: gmall-parent
 * @ClassName StartUpLog
 * @description:
 * @author: lzc
 * @create: 2020-03-09 14:41
 * @Version 1.0
 **/
case class StartUpLog(mid:String,
                      uid:String,
                      appid:String,
                      area:String,
                      os:String,
                      ch:String,
                      logType:String,
                      vs:String,
                      var logDate:String,
                      var logHour:String,
                      var ts:Long
                     ) {}