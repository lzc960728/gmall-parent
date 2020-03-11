package com.lzc.gmall.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.lzc.gmall.publisher.service.PublisherService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @program: gmall-parent
 * @ClassName PublisherController
 * @description:
 * @author: lzc
 * @create: 2020-03-10 17:59
 * @Version 1.0
 **/
@RestController
public class PublisherController {

    @Autowired
    private PublisherService publisherService;

    @GetMapping("realtime-total")
    public String getTotal(@RequestParam("date") String date) {
        Long dauTotal = publisherService.getDauTotal(date);
        List<Map> maps = new ArrayList<>();
        Map dauMap = new HashMap();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", dauTotal);
        Map newMidMap = new HashMap();
        newMidMap.put("id", "new_mid");
        newMidMap.put("name", "新增设备");
        newMidMap.put("value", 233);
        Map orderMap = new HashMap();
        orderMap.put("id", "order_amount");
        orderMap.put("name", "新增交易额");
        orderMap.put("value", publisherService.getOrderAmount(date));
        maps.add(dauMap);
        maps.add(newMidMap);
        maps.add(orderMap);
        return JSON.toJSONString(maps);
    }


    @GetMapping("realtime-hour")
    public String getDauHourCount(@RequestParam("id") String id, @RequestParam("date") String date) {
        Map resultMap = new HashMap(2);
        if ("order_amount".equals(id)) {
            Map<String, Double> todayOrderHourAmount = publisherService.getOrderAmountHour(date);
            Map<String, Double> yesterdayOrderHourAmount = publisherService.getOrderAmountHour(getYesterDayString(date));
            resultMap.put("today", todayOrderHourAmount);
            resultMap.put("yesterday", yesterdayOrderHourAmount);
            return JSON.toJSONString(resultMap);
        } else if ("dau".equals(id)) {
            Map<String, Long> toDayCount = publisherService.getDauHourCount(date);
            Map<String, Long> yesterDayCount = publisherService.getDauHourCount(getYesterDayString(date));
            resultMap.put("today", toDayCount);
            resultMap.put("yesterday", yesterDayCount);
            return JSON.toJSONString(resultMap);
        }
        return null;
    }

    /**
     * 获取前一天的私有方法
     *
     * @param date
     * @return
     */
    private String getYesterDayString(String date) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String yesterDayString = null;
        try {
            Date today = dateFormat.parse(date);
            Date yesterDay = DateUtils.addDays(today, -1);
            yesterDayString = dateFormat.format(yesterDay);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return yesterDayString;
    }


}
