package com.atguigu.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.constants.GmallConstants
import com.atguigu.handler.DauHandler
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author HP  
 * @Date: 2021-11-30 19:32
 * @Description:
 */
object DauApp1 {
  def main(args: Array[String]): Unit = {
    //1.创建配置文件
    val conf = new SparkConf().setAppName("DauApp1").setMaster("local[*]")

    //2.创建StreamingContext
    val scc = new StreamingContext(conf, Seconds(5))

    //3.获取kafka数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, scc)

    //4.将json格式转换成样例类,并补充字段
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val startUpLogDStream = kafkaDStream.mapPartitions(partiton => {
      partiton.map(record => {
        val startUpLog = JSON.parseObject(record.value(), classOf[StartUpLog])
        val times: String = sdf.format(new Date(startUpLog.ts))

        startUpLog.logDate = times.split(" ")(0)
        startUpLog.logHour = times.split(" ")(1)

        startUpLog
      })
    })

    //5.进行批次间去重
    val filterByRedisDStream = DauHandler.filterByRedis(startUpLogDStream)
    //6.进行批次内去重

    //7.将去重的(mid)保存到Redis
    DauHandler.saveToRedis(filterByRedisDStream)


    //8.将明细数据保存到HBASE


    //9.线程阻塞
    scc.start()
    scc.awaitTermination()
  }
}
