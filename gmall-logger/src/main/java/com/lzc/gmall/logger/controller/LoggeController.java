package com.lzc.gmall.logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lzc.gmall.common.GmallConstants;
import com.lzc.gmall.logger.GmallLoggerApplication;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;


/**
 * @program: gmall-parent
 * @ClassName LoggeController
 * @description:
 * @author: lzc
 * @create: 2020-03-06 17:27
 * @Version 1.0
 **/
@Slf4j
@RestController
public class LoggeController {


    @Autowired
    KafkaTemplate<String,String> kafkaTemplate;

    @PostMapping("log")
    public String dolog(@RequestParam("logString") String logString){
        //1.补充时间戳
        JSONObject jsonObject = JSON.parseObject(logString);
        jsonObject.put("ts",System.currentTimeMillis());
        //2.落盘
        String jsonString = jsonObject.toJSONString();
        log.info(jsonString);
        //3.推送到kafka
        if("startup".equals(jsonObject.getString("type"))){
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_STARTUP,jsonString);
        }else{
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_EVENT,jsonString);
        }

        return "success";
    }
}
