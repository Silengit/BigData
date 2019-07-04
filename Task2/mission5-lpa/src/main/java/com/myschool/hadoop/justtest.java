package com.myschool.hadoop;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class justtest {
    public static void main(String args[]) {
        HashMap<Double,String> map = new HashMap<Double,String>();
        map.put(1.123,"你好");
        map.put(1.0, "大家好");
        map.put(2.0, "我们很快乐");
        
        System.out.println(map.values().contains("你好"));

        List<HashMap.Entry<Double, String>> list = new ArrayList<HashMap.Entry<Double, String>>(map.entrySet());
        Comparator<HashMap.Entry<Double, String>> com = new Comparator<HashMap.Entry<Double, String>>() {
            public int compare(HashMap.Entry<Double, String> o1, HashMap.Entry<Double, String> o2) {
                return -o1.getKey().compareTo(o2.getKey());
            }
        };
        Collections.sort(list, com);
        for (Map.Entry<Double, String> mapping : list) {
            System.out.println(mapping.getKey() + " " + mapping.getValue());
        }
    }
}