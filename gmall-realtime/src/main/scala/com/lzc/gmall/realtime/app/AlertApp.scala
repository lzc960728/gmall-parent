package com.lzc.gmall.realtime.app

import com.alibaba.fastjson.JSON
import com.lzc.gmall.common.GmallConstants
import com.lzc.gmall.realtime.bean.{CouponAlertInfo, EventInfo}
import com.lzc.gmall.realtime.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.util

import org.apache.spark.rdd.RDD

import scala.util.control.Breaks._

/**
 * @program: gmall-parent
 * @ClassName AlertApp
 * @description:
 * @author: lzc
 * @create: 2020-03-11 19:06
 * @Version 1.0
 **/
object AlertApp {
  //  拆 :
  //    数据来源 : event日志 {格式}
  //  同一设备 : mid
  //  5分钟内  : 窗口大小 5分钟  窗口大小:数据范围 滑动步长 : 统计频率
  //  用不同账号登录并领取优惠劵且没有浏览商品 : groupbykey filter
  //    产生一条预警日志{格式} : map(转换格式)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("alert_app").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))
    val inputDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT, ssc)

    // 1. 转换结构便于后续操作
    val eventInfoDStream = inputDStream.map {
      record => {
        val eventInfo = JSON.parseObject(record.value(), classOf[EventInfo])
        eventInfo
      }
    }

    // 2. 5分钟内  : 窗口大小 5分钟  窗口大小:数据范围 滑动步长 : 统计频率
    val windowDStream: DStream[EventInfo] = eventInfoDStream.window(Seconds(300), Seconds(5))


    // 3. 同一设备 mid groupByKey
    val groupByMidDStream: DStream[(String, Iterable[EventInfo])] = windowDStream.map(eventInfo => (eventInfo.mid, eventInfo)).groupByKey()

    // 4. 用不同账号登录并领取优惠劵且没有浏览商品 : groupbykey filter
    val checkDStream: DStream[(Boolean, CouponAlertInfo)] = groupByMidDStream.map {
      case (mid, eventInfoItr) => {


        val couponUidSet = new util.HashSet[String]()
        val itemsSet = new util.HashSet[String]()
        val eventList = new util.ArrayList[String]()

        //        uids	领取优惠券登录过的uid
        //          itemIds	优惠券涉及的商品id
        //          events  	发生过的行为

        // 做一个浏览商品的标记
        var isClickItem = false
        // 判断用户是否有浏览商品的行为
        breakable {
          for (elem <- eventInfoItr) {
            eventList.add(elem.evid)
            if (elem.evid == "coupon") {
              // 点击了购物券需要拿到登录的用户
              couponUidSet.add(elem.uid)
              itemsSet.add(elem.itemid)
            }
            if (elem.evid == "clickItem") {
              isClickItem = true
              break()
            }
          }
        }
        // 需求：同一设备，5分钟内三次及以上用不同账号登录并领取优惠劵，
        // 并且在登录到领劵过程中没有浏览商品。达到以上要求则产生一条预警日志。同一设备，每分钟只记录一次预警。
        (couponUidSet.size() >= 3 && !isClickItem, CouponAlertInfo(mid, couponUidSet, itemsSet, eventList, System.currentTimeMillis()))
      }
    }
    //        checkDStream.foreachRDD{
    //          rdd => println(rdd.collect().mkString("\n"))
    //        }

    //    (true,CouponAlertInfo(mid_34,[110, 137, 58],[33, 3, 26, 49, 28, 7, 31, 20, 32],[addComment, addFavor, coupon, addComment, addComment, addFavor, coupon, coupon, addCart, coupon, coupon, addFavor, addComment, coupon, coupon, addComment, addCart, addCart, addCart, coupon, coupon, coupon],1583927285162))
    //    (true,CouponAlertInfo(mid_7,[100, 181, 36, 124, 29],[0, 44, 34, 1, 45, 23, 47, 14, 8, 41, 10],[addCart, addCart, addFavor, addComment, coupon, addCart, addCart, addCart, addComment, coupon, coupon, coupon, coupon, coupon, addComment, addCart, addComment, coupon, coupon, coupon, addComment, coupon, addCart, coupon, coupon, addFavor, coupon, addCart, addComment, addCart, coupon, addCart, addCart, addCart, addCart, addComment, addCart],1583927285162))
    //    (false,CouponAlertInfo(mid_23,[194, 152],[23, 20, 32],[addCart, addFavor, addComment, coupon, coupon, addCart, addFavor, coupon, addCart],1583927285162))
    //    (true,CouponAlertInfo(mid_12,[42, 25, 58],[22, 35, 24, 3, 37, 15, 19],[addComment, coupon, addFavor, coupon, coupon, addComment, addComment, addCart, addComment, addCart, addCart, addComment, addCart, coupon, addFavor, addFavor, coupon, coupon, addComment, coupon, coupon, coupon],1583927285162))
    //    (true,CouponAlertInfo(mid_13,[169, 127, 18],[22, 45, 1, 13, 47, 49, 39],[coupon, addCart, coupon, addFavor, addCart, coupon, addCart, addCart, addCart, addComment, coupon, coupon, coupon, addComment, addCart, coupon],1583927285175))

    // 5. 多变少 groupbykey filter
    val alterDStream: DStream[CouponAlertInfo] = checkDStream.filter(_._1).map(_._2)


    //    CouponAlertInfo(mid_34,[110, 137, 58],[33, 3, 26, 49, 28, 7, 31, 20, 32],[addComment, addFavor, coupon, addComment, addComment, addFavor, coupon, coupon, addCart, coupon, coupon, addFavor, addComment, coupon, coupon, addComment, addCart, addCart, addCart, coupon, coupon, coupon],1583927285196)
    //    CouponAlertInfo(mid_7,[100, 181, 36, 124, 29],[0, 44, 34, 1, 45, 23, 47, 14, 8, 41, 10],[addCart, addCart, addFavor, addComment, coupon, addCart, addCart, addCart, addComment, coupon, coupon, coupon, coupon, coupon, addComment, addCart, addComment, coupon, coupon, coupon, addComment, coupon, addCart, coupon, coupon, addFavor, coupon, addCart, addComment, addCart, coupon, addCart, addCart, addCart, addCart, addComment, addCart],1583927285196)
    //    CouponAlertInfo(mid_12,[42, 25, 58],[22, 35, 24, 3, 37, 15, 19],[addComment, coupon, addFavor, coupon, coupon, addComment, addComment, addCart, addComment, addCart, addCart, addComment, addCart, coupon, addFavor, addFavor, coupon, coupon, addComment, coupon, coupon, coupon],1583927285196)
    //    CouponAlertInfo(mid_13,[169, 127, 18],[22, 45, 1, 13, 47, 49, 39],[coupon, addCart, coupon, addFavor, addCart, coupon, addCart, addCart, addCart, addComment, coupon, coupon, coupon, addComment, addCart, coupon],1583927285197)
    //    CouponAlertInfo(mid_35,[169, 183, 28, 86],[33, 35, 24, 18, 8, 40, 30, 21],[addFavor, addCart, addCart, addComment, addCart, coupon, addComment, addComment, coupon, addFavor, coupon, addCart, addFavor, coupon, coupon, coupon, coupon, coupon, addComment],1583927285197)


    ssc.start()
    ssc.awaitTermination()
  }

}
