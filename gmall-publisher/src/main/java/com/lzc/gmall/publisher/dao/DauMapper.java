package com.lzc.gmall.publisher.dao;

import java.util.List;
import java.util.Map;

public interface DauMapper {
    // 查询日活总数
    Long getDauTotal(String date);

    // 分时查询
    List<Map> getDauHourCount(String date);
}
