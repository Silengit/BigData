package com.myschool.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashSet;

public class justtest {
    public static void main(String[] args) throws IOException {
        HashSet set = new HashSet();
        InputStream s = justtest.class.getClassLoader().getResourceAsStream("library/result.dic");
        Reader reader = new InputStreamReader(s);
        BufferedReader in = new BufferedReader(reader);
        String line = null;
        while ((line = in.readLine()) != null) {
            System.out.println(line.split("\\s+")[0]);
        }
        
        // set.add("")
    }
}