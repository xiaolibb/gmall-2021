package com.atguigu.handler

import com.atguigu.bean.StartUpLog
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

/**
 * @Author HP  
 * @Date: 2021-11-30 20:18
 * @Description:
 */
object DauHandler1 {
  def filterByRedis(startUpLogDStream: DStream[StartUpLog]) = {
    /* filter算子会对rdd进行过滤也就会多次连接
    val value = startUpLogDStream.filter(startUpLog => {
      //因为是在Redis里面进行过滤(去重)
      //1.获取Redis连接
      val jedis = new Jedis("hadoop102", 6379)

      //设计key
      val redisKey = "DAU" + startUpLog.logDate

      //进行判断来看有没有(也就是去重)(没有就要)
      val boolean = jedis.sismember(redisKey, startUpLog.mid)

      jedis.close()
      !boolean
    })
    value
     */

    //方案2:每一个分区下创建一次连接
    val value = startUpLogDStream.mapPartitions(partition => {
      //在分区下创建Redis连接
      val jedis = new Jedis("hadoop102", 6379)
      val logs = partition.filter(startUpLog => {
        //设计key
        val redisKey = "DAU" + startUpLog.logDate

        //进行判断来看有没有(也就是去重)(没有就要)
        val boolean = jedis.sismember(redisKey, startUpLog.mid)

        !boolean
      })
      jedis.close()
      logs
    })
    value
  }


  def saveToRedis(startUpLogDStream: DStream[StartUpLog]) = {
    startUpLogDStream.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        //在每个分区下创建Redis连接
        val jedis = new Jedis("hadoop102", 6379)

        partition.foreach(log => {
          val redisKey = "DAU" + log.logDate
          jedis.sadd(redisKey, log.mid)
          redisKey
        })
        jedis.close()
      })
    })
  }
}
