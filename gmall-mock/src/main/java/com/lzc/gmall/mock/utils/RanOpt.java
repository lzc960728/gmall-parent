package com.lzc.gmall.mock.utils;

/**
 * @program: gmall-parent
 * @ClassName RanOpt
 * @description:
 * @author: lyy
 * @create: 2020-03-06 16:25
 * @Version 1.0
 **/
public class RanOpt<T>{
    T value ;
    int weight;

    public RanOpt ( T value, int weight ){
        this.value=value ;
        this.weight=weight;
    }

    public T getValue() {
        return value;
    }

    public int getWeight() {
        return weight;
    }
}
