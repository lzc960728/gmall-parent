package com.lzc.gmall.publisher.service;

import java.util.Map;

public interface PublisherService {
    // 查询日活总数
    Long getDauTotal(String date);

    // 分时查询
    Map<String, Long> getDauHourCount(String date);

     Double getOrderAmount(String date);

     Map getOrderAmountHour(String date);

}
