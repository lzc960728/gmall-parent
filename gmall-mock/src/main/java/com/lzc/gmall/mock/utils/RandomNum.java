package com.lzc.gmall.mock.utils;

import java.util.Random;

/**
 * @program: gmall-parent
 * @ClassName RandomNum
 * @description:
 * @author: lyy
 * @create: 2020-03-06 16:27
 * @Version 1.0
 **/
public class RandomNum {
    public static final  int getRandInt(int fromNum,int toNum){
        return   fromNum+ new Random().nextInt(toNum-fromNum+1);
    }
}
