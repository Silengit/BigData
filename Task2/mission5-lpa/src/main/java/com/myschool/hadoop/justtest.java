package com.myschool.hadoop;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class justtest {
    public static void main(String args[]) {
        // System.out.println(Double.parseDouble("4.5392648E-4"));
        // String value_name = "[大圣,9.3720714E-4|孙悟空,0.2222]";
        // String list[] = value_name.substring(1, value_name.length() - 1).split("\\|");
        // for (String var : list) {
        //     System.out.println(var + " " + Double.parseDouble(var.split(",")[1]));
        // }
        HashMap<String,Double> map = new HashMap<String,Double>();
        map.put("你好",1.123);
        map.put("大家好",1.0);
        map.put("我们很快乐",2.0);
        
        System.out.println(map.keySet().contains("你好"));
        map.put("你好", map.get("你好") + 3);

        List<HashMap.Entry<String,Double>> list = new ArrayList<HashMap.Entry<String,Double>>(map.entrySet());
        Comparator<HashMap.Entry<String,Double>> com = new Comparator<HashMap.Entry<String,Double>>() {
            public int compare(HashMap.Entry<String,Double> o1, HashMap.Entry<String,Double> o2) {
                return -o1.getValue().compareTo(o2.getValue());
            }
        };
        Collections.sort(list, com);
        for (HashMap.Entry<String,Double> mapping : list) {
            System.out.println(mapping.getKey() + " " + mapping.getValue());
        }
    }
}