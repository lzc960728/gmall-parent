package com.lzc.gmall.publisher.dao;

import java.util.List;
import java.util.Map;

/**
 * @program: gmall-parent
 * @ClassName OrderMapper
 * @description:
 * @author: lzc
 * @create: 2020-03-11 16:58
 * @Version 1.0
 **/
public interface OrderMapper {
    //1 查询当日交易额总数

    public Double selectOrderAmountTotal(String date);

    //2 查询当日交易额分时明细
    public List<Map> selectOrderAmountHourMap(String date);

}
