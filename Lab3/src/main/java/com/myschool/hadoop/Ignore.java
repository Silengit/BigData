package com.myschool.hadoop;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;

class Ignore {
    private Set<String> ignored = new HashSet<String>();

    public Ignore() {
        loadfromFIle();
    }

    public boolean contains(String s){
        return ignored.contains(s);
    }
    public Set<String> loadfromFIle() {
        // HashSet 对于 判断 这个集合是否包含某个元素非常快
        InputStream s;
        InputStreamReader inputReader;
        try {
            s = this.getClass().getClassLoader().getResourceAsStream("ignore.txt");

            inputReader = new InputStreamReader(s);
            BufferedReader bf = new BufferedReader(inputReader);
            String str;
            while ((str = bf.readLine()) != null) {
                // System.out.println(str);
                ignored.add(str);
            }
            bf.close();
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return ignored;
    }
}
