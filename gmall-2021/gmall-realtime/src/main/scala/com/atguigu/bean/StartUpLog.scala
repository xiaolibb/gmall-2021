package com.atguigu.bean

/**
 * @Author HP  
 * @Date: 2021-11-30 14:32
 * @Description:
 */
case class StartUpLog(mid:String,
                      uid:String,
                      appid:String,
                      area:String,
                      os:String,
                      ch:String,
                      `type`:String,
                      vs:String,
                      var logDate:String,
                      var logHour:String,
                      var ts:Long)