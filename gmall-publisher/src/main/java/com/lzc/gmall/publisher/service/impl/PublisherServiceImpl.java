package com.lzc.gmall.publisher.service.impl;

import com.lzc.gmall.publisher.dao.DauMapper;
import com.lzc.gmall.publisher.dao.OrderMapper;
import com.lzc.gmall.publisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
/**
 * @program: gmall-parent
 * @ClassName PublisherServiceImpl
 * @description:
 * @author: lzc
 * @create: 2020-03-10 17:56
 * @Version 1.0
 **/
@Service
public class PublisherServiceImpl implements PublisherService {
    @Autowired
    private DauMapper dauMapper;

    @Autowired
    private OrderMapper OrderMapper;


    /**
     * 查询日活总数
     *
     * @param date
     * @return
     */
    @Override
    public Long getDauTotal(String date) {
        return dauMapper.getDauTotal(date);
    }

    /**
     * 获取分时数据
     *
     * @param date
     * @return
     */
    @Override
    public Map<String, Long> getDauHourCount(String date) {
        List<Map> dauHourCount = dauMapper.getDauHourCount(date);
        Map<String, Long> hashMap = new HashMap(dauHourCount.size());
        dauHourCount.forEach(data -> hashMap.put((String) data.get("LH"), (Long) data.get("CT")));
        return hashMap;
    }
    @Override
    public Map getOrderAmountHour(String date) {
        List<Map> mapList = OrderMapper.selectOrderAmountHourMap(date);
        Map orderAmountHourMap = new HashMap();
        for (Map map:mapList){
            orderAmountHourMap.put(map.get("CREATE_HOUR"), map.get("SUM_AMOUNT"));
        }
        return orderAmountHourMap;
    }

    @Override
    public Double getOrderAmount(String date) {
        return OrderMapper.selectOrderAmountTotal(date);
    }


}
