package com.lzc.gmall.mock.utils;

import java.util.Date;
import java.util.Random;

/**
 * @program: gmall-parent
 * @ClassName RandomDate
 * @description:
 * @author: lyy
 * @create: 2020-03-06 16:25
 * @Version 1.0
 **/
public class RandomDate {
    Long logDateTime =0L;//
    int maxTimeStep=0 ;


    public RandomDate (Date startDate , Date  endDate, int num) {

        Long avgStepTime = (endDate.getTime()- startDate.getTime())/num;
        this.maxTimeStep=avgStepTime.intValue()*2;
        this.logDateTime=startDate.getTime();

    }


    public  Date  getRandomDate() {
        int  timeStep = new Random().nextInt(maxTimeStep);
        logDateTime = logDateTime+timeStep;
        return new Date( logDateTime);
    }

}
